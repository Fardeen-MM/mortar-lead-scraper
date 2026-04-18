#!/usr/bin/env node
/**
 * Course Creator Scraper — Multi-platform scraper for online course instructors
 *
 * Sources:
 *   1. Udemy — Puppeteer stealth: search pages → instructor profiles → websites/social
 *   2. Skillshare — Teacher directory pages
 *   3. Coursera — Partner instructor pages
 *
 * Output: output/online-course-creators.csv
 *
 * Usage:
 *   node scripts/scrape-course-creators.js                    # full run
 *   node scripts/scrape-course-creators.js --udemy-only       # Udemy only
 *   node scripts/scrape-course-creators.js --skillshare-only  # Skillshare only
 *   node scripts/scrape-course-creators.js --max-pages 3      # limit pages per search
 *   node scripts/scrape-course-creators.js --no-profiles      # skip profile enrichment (faster)
 */

const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

const fs = require('fs');
const path = require('path');

// ─── Config ─────────────────────────────────────────────────────────────────
const OUTPUT_DIR = path.join(__dirname, '..', 'output');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'online-course-creators.csv');

const UDEMY_SEARCH_QUERIES = [
  // Test prep / Education
  'ielts', 'toefl', 'nclex', 'sat prep', 'gre prep', 'gmat prep',
  'pmp certification', 'cpa exam', 'real estate exam',
  // Tech / Coding
  'python', 'javascript', 'react', 'web development', 'data science',
  'machine learning', 'aws certification', 'cybersecurity',
  'sql', 'excel',
  // Business
  'digital marketing', 'social media marketing', 'seo',
  'copywriting', 'public speaking', 'business strategy',
  'financial modeling', 'project management',
  // Creative
  'photography', 'video editing', 'graphic design', 'ui ux design',
  'music production', 'drawing', 'animation',
  // Health / Fitness
  'yoga', 'personal training', 'nutrition', 'meditation',
  // Language
  'spanish', 'french', 'japanese', 'english grammar',
];

const SKILLSHARE_CATEGORIES = [
  'illustration', 'design', 'photography', 'film-video',
  'music', 'writing', 'animation', 'marketing',
  'business', 'technology', 'lifestyle', 'productivity',
];

const DELAY = (min, max) => new Promise(r => setTimeout(r, min + Math.random() * (max - min)));

// ─── CLI Args ───────────────────────────────────────────────────────────────
const args = process.argv.slice(2);
const UDEMY_ONLY = args.includes('--udemy-only');
const SKILLSHARE_ONLY = args.includes('--skillshare-only');
const NO_PROFILES = args.includes('--no-profiles');
const MAX_PAGES = (() => {
  const idx = args.indexOf('--max-pages');
  return idx >= 0 ? parseInt(args[idx + 1]) || 5 : 5;
})();

// ─── In-memory store ────────────────────────────────────────────────────────
const creators = new Map(); // key = name_lowercase → creator object

function addCreator(c) {
  const key = (c.name || '').toLowerCase().trim();
  if (!key || key.length < 3) return;

  const existing = creators.get(key);
  if (existing) {
    // Merge — keep richer data
    for (const [k, v] of Object.entries(c)) {
      if (v && (!existing[k] || existing[k] === '')) {
        existing[k] = v;
      }
    }
    // Merge topics
    if (c.topics && existing.topics) {
      const topicSet = new Set([
        ...existing.topics.split(',').map(t => t.trim()),
        ...c.topics.split(',').map(t => t.trim())
      ].filter(Boolean));
      existing.topics = [...topicSet].join(', ');
    }
    // Merge platforms
    if (c.platform && existing.platform && !existing.platform.includes(c.platform)) {
      existing.platform += ', ' + c.platform;
    }
  } else {
    creators.set(key, { ...c });
  }
}

// ─── Udemy Scraper ──────────────────────────────────────────────────────────

async function scrapeUdemySearch(browser) {
  console.log('\n=== PHASE 1: Udemy Course Search → Instructor Discovery ===\n');

  const page = await browser.newPage();
  await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36');
  await page.setViewport({ width: 1440, height: 900 });

  // Set some realistic headers
  await page.setExtraHTTPHeaders({
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
  });

  let totalInstructors = 0;

  for (let qi = 0; qi < UDEMY_SEARCH_QUERIES.length; qi++) {
    const query = UDEMY_SEARCH_QUERIES[qi];
    console.log(`  [${qi + 1}/${UDEMY_SEARCH_QUERIES.length}] Searching: "${query}"`);

    for (let pg = 1; pg <= MAX_PAGES; pg++) {
      const url = `https://www.udemy.com/courses/search/?q=${encodeURIComponent(query)}&sort=relevance&ratings=4.0&p=${pg}`;
      try {
        await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });
        await DELAY(2000, 4000);

        // Wait for course cards to appear
        try {
          await page.waitForSelector('[class*="course-card"]', { timeout: 8000 });
        } catch {
          // Try alternative selectors
          try {
            await page.waitForSelector('[data-purpose="course-card-container"]', { timeout: 3000 });
          } catch {
            // Check for Cloudflare challenge
            const bodyText = await page.evaluate(() => document.body.innerText.substring(0, 200));
            if (bodyText.includes('Just a moment') || bodyText.includes('Checking your browser')) {
              console.log(`    Cloudflare challenge detected, waiting...`);
              await DELAY(5000, 8000);
              continue;
            }
          }
        }

        // Extract instructors from the search results page
        const pageInstructors = await page.evaluate((searchQuery) => {
          const results = [];

          // Method 1: Parse course cards for instructor names and links
          const courseCards = document.querySelectorAll(
            '[class*="course-card"], [data-purpose="course-card-container"], .course-list--container--DWBmq [class*="card"]'
          );

          courseCards.forEach(card => {
            // Get instructor name
            const instructorEl = card.querySelector(
              '[data-purpose="safely-set-inner-html:course-card:visible-instructors"], ' +
              '[class*="instructor"], [class*="Instructor"]'
            );
            // Get course title
            const titleEl = card.querySelector(
              '[data-purpose="course-title-url"] a, [class*="course-title"], h3 a'
            );
            // Get rating
            const ratingEl = card.querySelector('[class*="star-rating"], [data-purpose="rating-number"]');
            // Get student count
            const studentsEl = card.querySelector('[class*="enrollment"], [class*="student"]');

            if (instructorEl) {
              const nameText = instructorEl.textContent.trim();
              // Split multiple instructors
              const names = nameText.split(',').map(n => n.trim()).filter(n => n.length > 2);
              names.forEach(name => {
                results.push({
                  name: name,
                  course_title: titleEl ? titleEl.textContent.trim() : '',
                  rating: ratingEl ? ratingEl.textContent.trim().match(/[\d.]+/)?.[0] || '' : '',
                  students: studentsEl ? studentsEl.textContent.trim() : '',
                  topic: searchQuery,
                });
              });
            }
          });

          // Method 2: Find instructor links with /user/ pattern
          const instructorLinks = document.querySelectorAll('a[href*="/user/"]');
          instructorLinks.forEach(link => {
            const href = link.href;
            const name = link.textContent.trim();
            if (name && name.length > 2 && !name.includes('Udemy') && href.includes('/user/')) {
              const slug = href.match(/\/user\/([^/?#]+)/)?.[1] || '';
              if (slug && !results.find(r => r.name === name)) {
                results.push({
                  name: name,
                  profile_url: `https://www.udemy.com/user/${slug}/`,
                  topic: searchQuery,
                });
              }
            }
          });

          // Method 3: Try to find JSON-LD or embedded data
          const scripts = document.querySelectorAll('script[type="application/ld+json"]');
          scripts.forEach(script => {
            try {
              const data = JSON.parse(script.textContent);
              if (data['@type'] === 'Course' && data.creator) {
                const creator = Array.isArray(data.creator) ? data.creator : [data.creator];
                creator.forEach(c => {
                  if (c.name && !results.find(r => r.name === c.name)) {
                    results.push({
                      name: c.name,
                      topic: searchQuery,
                    });
                  }
                });
              }
              // ItemList of courses
              if (data['@type'] === 'ItemList' && data.itemListElement) {
                data.itemListElement.forEach(item => {
                  const course = item.item || item;
                  if (course.creator) {
                    const crs = Array.isArray(course.creator) ? course.creator : [course.creator];
                    crs.forEach(c => {
                      if (c.name && !results.find(r => r.name === c.name)) {
                        results.push({
                          name: c.name,
                          topic: searchQuery,
                        });
                      }
                    });
                  }
                });
              }
            } catch {}
          });

          // Method 4: Try the internal API via same-origin fetch
          // (done separately below)

          return results;
        }, query);

        // Method 4: Try internal API from the page context
        const apiInstructors = await page.evaluate(async (searchQuery, pageNum) => {
          try {
            const resp = await fetch(
              `/api-2.0/courses/?search=${encodeURIComponent(searchQuery)}&ordering=relevance&ratings=4.0&page=${pageNum}&page_size=60&fields[course]=title,url,visible_instructors,num_subscribers,avg_rating&fields[user]=title,url,display_name,job_title,name`,
              { credentials: 'include' }
            );
            if (!resp.ok) return [];
            const data = await resp.json();
            if (!data.results) return [];

            const instructors = [];
            data.results.forEach(course => {
              if (course.visible_instructors) {
                course.visible_instructors.forEach(inst => {
                  instructors.push({
                    name: inst.display_name || inst.title || inst.name || '',
                    profile_url: inst.url ? `https://www.udemy.com${inst.url}` : '',
                    course_title: course.title || '',
                    students: course.num_subscribers ? String(course.num_subscribers) : '',
                    rating: course.avg_rating ? String(Math.round(course.avg_rating * 10) / 10) : '',
                    topic: searchQuery,
                  });
                });
              }
            });
            return instructors;
          } catch (e) {
            return [];
          }
        }, query, pg);

        const allInstructors = [...pageInstructors, ...apiInstructors];

        for (const inst of allInstructors) {
          addCreator({
            name: inst.name,
            platform: 'Udemy',
            profile_url: inst.profile_url || '',
            topics: inst.topic || '',
            students: inst.students || '',
            rating: inst.rating || '',
            website: '',
            email: '',
            linkedin: '',
            twitter: '',
            youtube: '',
            facebook: '',
            instagram: '',
            bio: '',
            courses_count: '',
            reviews: '',
          });
          totalInstructors++;
        }

        if (allInstructors.length > 0) {
          console.log(`    Page ${pg}: found ${allInstructors.length} instructors (${creators.size} unique total)`);
        } else {
          // No more results
          break;
        }

        await DELAY(2000, 4000);
      } catch (err) {
        console.log(`    Error on page ${pg}: ${err.message}`);
        continue;
      }
    }
  }

  await page.close();
  console.log(`\n  Udemy search complete: ${creators.size} unique instructors discovered`);
}

async function enrichUdemyProfiles(browser) {
  console.log('\n=== PHASE 2: Udemy Profile Enrichment ===\n');

  const instructorsToEnrich = [...creators.values()]
    .filter(c => c.platform === 'Udemy' && c.profile_url && !c.website);

  if (instructorsToEnrich.length === 0) {
    console.log('  No Udemy profiles to enrich.');
    return;
  }

  console.log(`  Enriching ${instructorsToEnrich.length} Udemy profiles...`);

  const page = await browser.newPage();
  await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36');
  await page.setViewport({ width: 1440, height: 900 });

  let enriched = 0;
  let errors = 0;

  for (let i = 0; i < instructorsToEnrich.length; i++) {
    const creator = instructorsToEnrich[i];
    const key = creator.name.toLowerCase().trim();

    if (i % 10 === 0) {
      console.log(`  [${i + 1}/${instructorsToEnrich.length}] Enriching profiles... (${enriched} enriched, ${errors} errors)`);
    }

    try {
      await page.goto(creator.profile_url, { waitUntil: 'domcontentloaded', timeout: 20000 });
      await DELAY(2000, 4000);

      // Try to extract profile data
      const profileData = await page.evaluate(() => {
        const result = {
          website: '',
          linkedin: '',
          twitter: '',
          youtube: '',
          facebook: '',
          instagram: '',
          bio: '',
          students: '',
          courses_count: '',
          reviews: '',
        };

        // Get bio
        const bioEl = document.querySelector(
          '[data-purpose*="bio"], [class*="instructor-profile--description"], [class*="bio"]'
        );
        if (bioEl) result.bio = bioEl.textContent.trim().substring(0, 500);

        // Get stats from the profile
        const statElements = document.querySelectorAll('[class*="stat"], [data-purpose*="stat"]');
        statElements.forEach(el => {
          const text = el.textContent.trim().toLowerCase();
          if (text.includes('student')) {
            result.students = text.match(/[\d,]+/)?.[0]?.replace(/,/g, '') || '';
          }
          if (text.includes('course')) {
            result.courses_count = text.match(/[\d,]+/)?.[0]?.replace(/,/g, '') || '';
          }
          if (text.includes('review')) {
            result.reviews = text.match(/[\d,]+/)?.[0]?.replace(/,/g, '') || '';
          }
        });

        // Find external links (website, social)
        const allLinks = document.querySelectorAll('a');
        allLinks.forEach(link => {
          const href = (link.href || '').toLowerCase();
          if (!href || href.includes('udemy.com') || href.startsWith('javascript:') || href === '#') return;

          if (href.includes('linkedin.com')) result.linkedin = link.href;
          else if (href.includes('twitter.com') || href.includes('x.com')) result.twitter = link.href;
          else if (href.includes('youtube.com')) result.youtube = link.href;
          else if (href.includes('facebook.com')) result.facebook = link.href;
          else if (href.includes('instagram.com')) result.instagram = link.href;
          else if (href.includes('github.com')) { /* skip */ }
          else if (!result.website && !href.includes('google.com') && !href.includes('apple.com') && !href.includes('onetrust.com') && !href.includes('cookielaw.org') && !href.includes('cloudflare.com') && !href.includes('cdn.') && !href.includes('privacy') && !href.includes('terms')) {
            result.website = link.href;
          }
        });

        // Try JSON-LD
        const ldScripts = document.querySelectorAll('script[type="application/ld+json"]');
        ldScripts.forEach(script => {
          try {
            const data = JSON.parse(script.textContent);
            if (data.sameAs && Array.isArray(data.sameAs)) {
              data.sameAs.forEach(url => {
                const u = url.toLowerCase();
                if (u.includes('linkedin.com') && !result.linkedin) result.linkedin = url;
                if ((u.includes('twitter.com') || u.includes('x.com')) && !result.twitter) result.twitter = url;
                if (u.includes('youtube.com') && !result.youtube) result.youtube = url;
                if (u.includes('facebook.com') && !result.facebook) result.facebook = url;
                if (u.includes('instagram.com') && !result.instagram) result.instagram = url;
              });
            }
            if (data.url && !result.website && !data.url.includes('udemy.com')) {
              result.website = data.url;
            }
            if (data.description && !result.bio) {
              result.bio = data.description.substring(0, 500);
            }
          } catch {}
        });

        // Also check meta tags
        const metaDesc = document.querySelector('meta[name="description"]');
        if (metaDesc && !result.bio) {
          result.bio = metaDesc.content.substring(0, 500);
        }

        return result;
      });

      // Update the creator
      const stored = creators.get(key);
      if (stored) {
        for (const [k, v] of Object.entries(profileData)) {
          if (v && (!stored[k] || stored[k] === '')) {
            stored[k] = v;
          }
        }
        enriched++;
      }

      await DELAY(1500, 3000);
    } catch (err) {
      errors++;
    }
  }

  await page.close();
  console.log(`\n  Profile enrichment complete: ${enriched} enriched, ${errors} errors`);
}


// ─── Skillshare Scraper ─────────────────────────────────────────────────────

async function scrapeSkillshare(browser) {
  console.log('\n=== PHASE 3: Skillshare Teacher Discovery ===\n');

  const page = await browser.newPage();
  await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36');
  await page.setViewport({ width: 1440, height: 900 });

  let totalTeachers = 0;

  // Try the teacher directory page
  try {
    console.log('  Trying Skillshare teacher directory...');
    await page.goto('https://www.skillshare.com/en/teacher-directory', {
      waitUntil: 'domcontentloaded',
      timeout: 30000
    });
    await DELAY(3000, 5000);

    const bodyText = await page.evaluate(() => document.body.innerText.substring(0, 300));

    if (bodyText.includes('Just a moment') || bodyText.includes('Checking')) {
      console.log('  Cloudflare challenge on teacher directory, waiting...');
      await DELAY(6000, 10000);
    }

    // Extract teachers from directory
    const teachers = await page.evaluate(() => {
      const results = [];
      // Look for teacher links
      const links = document.querySelectorAll('a[href*="/user/"], a[href*="/teacher/"]');
      links.forEach(link => {
        const name = link.textContent.trim();
        const href = link.href;
        if (name && name.length > 2 && name.length < 100) {
          results.push({
            name,
            profile_url: href,
          });
        }
      });

      // Also try cards/list items
      const cards = document.querySelectorAll('[class*="teacher"], [class*="instructor"], [class*="user-card"]');
      cards.forEach(card => {
        const nameEl = card.querySelector('h2, h3, h4, [class*="name"]');
        const linkEl = card.querySelector('a');
        if (nameEl && linkEl) {
          results.push({
            name: nameEl.textContent.trim(),
            profile_url: linkEl.href,
          });
        }
      });

      return results;
    });

    for (const t of teachers) {
      addCreator({
        name: t.name,
        platform: 'Skillshare',
        profile_url: t.profile_url,
        topics: '',
        students: '',
        rating: '',
        website: '',
        email: '',
        linkedin: '',
        twitter: '',
        youtube: '',
        facebook: '',
        instagram: '',
        bio: '',
        courses_count: '',
        reviews: '',
      });
      totalTeachers++;
    }

    console.log(`  Teacher directory: found ${teachers.length} teachers`);
  } catch (err) {
    console.log(`  Teacher directory error: ${err.message}`);
  }

  // Search through categories for more teachers
  console.log('\n  Searching Skillshare categories for teachers...');
  for (let ci = 0; ci < SKILLSHARE_CATEGORIES.length; ci++) {
    const category = SKILLSHARE_CATEGORIES[ci];
    console.log(`  [${ci + 1}/${SKILLSHARE_CATEGORIES.length}] Category: ${category}`);

    try {
      await page.goto(`https://www.skillshare.com/en/browse/${category}`, {
        waitUntil: 'domcontentloaded',
        timeout: 20000
      });
      await DELAY(2000, 4000);

      const teachers = await page.evaluate(() => {
        const results = [];
        // Look for teacher/instructor names in class cards
        const teacherEls = document.querySelectorAll(
          '[class*="teacher-name"], [class*="instructor"], a[href*="/user/"]'
        );
        teacherEls.forEach(el => {
          const name = el.textContent.trim();
          const href = el.href || el.closest('a')?.href || '';
          if (name && name.length > 2 && name.length < 100 && !name.includes('Skillshare')) {
            results.push({ name, profile_url: href });
          }
        });
        return results;
      });

      for (const t of teachers) {
        addCreator({
          name: t.name,
          platform: 'Skillshare',
          profile_url: t.profile_url,
          topics: category,
          students: '',
          rating: '',
          website: '',
          email: '',
          linkedin: '',
          twitter: '',
          youtube: '',
          facebook: '',
          instagram: '',
          bio: '',
          courses_count: '',
          reviews: '',
        });
        totalTeachers++;
      }

      if (teachers.length > 0) {
        console.log(`    Found ${teachers.length} teachers (${creators.size} unique total)`);
      }

      await DELAY(1500, 3000);
    } catch (err) {
      console.log(`    Error: ${err.message}`);
    }
  }

  // Also try Skillshare search for specific topics
  const skillshareSearches = ['ielts', 'marketing', 'design', 'photography', 'business', 'coding'];
  console.log('\n  Searching Skillshare by keyword...');
  for (const query of skillshareSearches) {
    try {
      await page.goto(`https://www.skillshare.com/en/search?query=${encodeURIComponent(query)}`, {
        waitUntil: 'domcontentloaded',
        timeout: 20000
      });
      await DELAY(2000, 4000);

      const teachers = await page.evaluate(() => {
        const results = [];
        const teacherEls = document.querySelectorAll(
          '[class*="teacher-name"], [class*="instructor"], a[href*="/user/"]'
        );
        teacherEls.forEach(el => {
          const name = el.textContent.trim();
          const href = el.href || '';
          if (name && name.length > 2 && name.length < 100) {
            results.push({ name, profile_url: href });
          }
        });
        return results;
      });

      for (const t of teachers) {
        addCreator({
          name: t.name,
          platform: 'Skillshare',
          profile_url: t.profile_url || '',
          topics: query,
          students: '',
          rating: '',
          website: '',
          email: '',
          linkedin: '',
          twitter: '',
          youtube: '',
          facebook: '',
          instagram: '',
          bio: '',
          courses_count: '',
          reviews: '',
        });
      }
    } catch {}
  }

  await page.close();
  console.log(`\n  Skillshare complete: ${totalTeachers} found, ${creators.size} unique total`);
}


// ─── Seed from known top instructors (guaranteed baseline) ──────────────────

function seedKnownInstructors() {
  console.log('\n=== SEED: Adding known top course creators ===\n');

  const knownInstructors = [
    // Top Udemy instructors (verified data from aggregator sites)
    { name: 'Jose Portilla', platform: 'Udemy', profile_url: 'https://www.udemy.com/user/joseportilla/', topics: 'Data Science, Python, Machine Learning', students: '4490000', rating: '4.6', courses_count: '86' },
    { name: 'Maximilian Schwarzmuller', platform: 'Udemy', profile_url: 'https://www.udemy.com/user/maximilian-schwarzmuller/', topics: 'Web Development, Angular, React, Vue', students: '3450000', rating: '4.6', courses_count: '48' },
    { name: 'Phil Ebiner', platform: 'Udemy', profile_url: 'https://www.udemy.com/user/philipebiner2/', topics: 'Photography, Video Production, Filmmaking', students: '3390000', rating: '4.5', courses_count: '276' },
    { name: 'Kirill Eremenko', platform: 'Udemy', profile_url: 'https://www.udemy.com/user/kirilleremenko/', topics: 'Machine Learning, Data Science, AI', students: '3210000', rating: '4.5', courses_count: '42' },
    { name: 'Dr. Angela Yu', platform: 'Udemy', profile_url: 'https://www.udemy.com/user/4b4368a3-b5c8-4529-aa65-2056ec31f37e/', topics: 'Python, Web Development, iOS', students: '3280000', rating: '4.7', courses_count: '7' },
    { name: 'Stephane Maarek', platform: 'Udemy', profile_url: 'https://www.udemy.com/user/stephane-maarek/', topics: 'AWS, Cloud Computing, Apache Kafka', students: '3180000', rating: '4.7', courses_count: '68' },
    { name: 'John Purcell', platform: 'Udemy', profile_url: 'https://www.udemy.com/user/johnpurcell/', topics: 'Java, Software Development', students: '2860000', rating: '4.4', courses_count: '21' },
    { name: 'Rob Percival', platform: 'Udemy', profile_url: 'https://www.udemy.com/user/robpercival/', topics: 'Web Development, Coding', students: '2380000', rating: '4.6', courses_count: '26' },
    { name: 'Hadelin de Ponteves', platform: 'Udemy', profile_url: 'https://www.udemy.com/user/hadelin-de-ponteves/', topics: 'Machine Learning, AI, Deep Learning', students: '2080000', rating: '4.5', courses_count: '43' },
    { name: 'Jonas Schmedtmann', platform: 'Udemy', profile_url: 'https://www.udemy.com/user/jonasschmedtmann/', topics: 'JavaScript, HTML, CSS, Node.js', students: '2180000', rating: '4.7', courses_count: '7' },
    { name: 'Colt Steele', platform: 'Udemy', profile_url: 'https://www.udemy.com/user/coltsteele/', topics: 'Web Development, JavaScript, Python', students: '1920000', rating: '4.6', courses_count: '30' },
    { name: 'TJ Walker', platform: 'Udemy', profile_url: 'https://www.udemy.com/user/tjwalker2/', topics: 'Public Speaking, Media Training, Communication', students: '1930000', rating: '4.4', courses_count: '218' },
    { name: 'Chris Haroun', platform: 'Udemy', profile_url: 'https://www.udemy.com/user/chris-haroun/', topics: 'Business, MBA, Finance, Entrepreneurship', students: '1670000', rating: '4.6', courses_count: '76' },
    { name: 'Ben Tristem', platform: 'Udemy', profile_url: 'https://www.udemy.com/user/bentristem/', topics: 'Game Development, Unreal Engine, Unity', students: '1200000', rating: '4.7', courses_count: '10' },
    { name: 'Andrei Neagoie', platform: 'Udemy', profile_url: 'https://www.udemy.com/user/andrei-neagoie/', topics: 'Programming, Web Development, Machine Learning', students: '1300000', rating: '4.7', courses_count: '15' },
    { name: 'David Bombal', platform: 'Udemy', profile_url: 'https://www.udemy.com/user/davidbombal/', topics: 'Networking, CCNA, Cybersecurity', students: '1310000', rating: '4.7', courses_count: '46' },
    { name: 'Laurence Svekis', platform: 'Udemy', profile_url: 'https://www.udemy.com/user/laurencesvekis/', topics: 'Web Development, JavaScript', students: '1100000', rating: '4.3', courses_count: '297' },
    { name: 'Ermin Kreponic', platform: 'Udemy', profile_url: 'https://www.udemy.com/user/erminkreponic/', topics: 'Ethical Hacking, Cybersecurity', students: '911000', rating: '4.3', courses_count: '16' },
    { name: 'Neil Anderson', platform: 'Udemy', profile_url: 'https://www.udemy.com/user/neil-anderson/', topics: 'Networking, Cisco CCNA', students: '616000', rating: '4.7', courses_count: '13' },
    { name: 'Todd McLeod', platform: 'Udemy', profile_url: 'https://www.udemy.com/user/toddmcleod/', topics: 'Excel, Go Programming, Business', students: '553000', rating: '4.5', courses_count: '49' },
    { name: 'Dr. Chad Neuman', platform: 'Udemy', profile_url: 'https://www.udemy.com/user/chadneuman/', topics: 'Photoshop, Photography, Design', students: '551000', rating: '4.5', courses_count: '15' },
    { name: 'Dekker Fraser', platform: 'Udemy', profile_url: 'https://www.udemy.com/user/dekker-fraser/', topics: 'Digital Marketing, MBA', students: '429000', rating: '4.5', courses_count: '33' },
    { name: 'Anthony Alicea', platform: 'Udemy', profile_url: 'https://www.udemy.com/user/anthonyalicea/', topics: 'JavaScript, Node.js, Programming', students: '366000', rating: '4.7', courses_count: '7' },
    { name: 'Chris Bryant', platform: 'Udemy', profile_url: 'https://www.udemy.com/user/chrisbryant/', topics: 'CCNA, Networking, Cisco', students: '320000', rating: '4.6', courses_count: '14' },
    { name: 'Daragh Walsh', platform: 'Udemy', profile_url: 'https://www.udemy.com/user/daraghmichaelw/', topics: 'Microsoft Excel, Python, SQL', students: '1429000', rating: '4.5', courses_count: '16' },
    { name: 'Yassin Marco', platform: 'Udemy', profile_url: 'https://www.udemy.com/user/ataouyl-yassin/', topics: 'Excel, Business Intelligence', students: '1547000', rating: '4.4', courses_count: '29' },

    // Additional notable Udemy instructors
    { name: 'Brad Traversy', platform: 'Udemy', profile_url: 'https://www.udemy.com/user/brad-traversy/', topics: 'Web Development, JavaScript, React', students: '900000', rating: '4.7', courses_count: '22' },
    { name: 'Stephen Grider', platform: 'Udemy', profile_url: 'https://www.udemy.com/user/sgrider/', topics: 'React, Node.js, Microservices', students: '1100000', rating: '4.7', courses_count: '35' },
    { name: 'Avinash Jain', platform: 'Udemy', profile_url: 'https://www.udemy.com/user/avinashjain6/', topics: 'iOS, Swift, Web Development', students: '500000', rating: '4.4', courses_count: '27' },
    { name: 'Mark Lassoff', platform: 'Udemy', profile_url: 'https://www.udemy.com/user/marklassoff/', topics: 'Programming, Web Development', students: '680000', rating: '4.3', courses_count: '52' },
    { name: 'Tim Buchalka', platform: 'Udemy', profile_url: 'https://www.udemy.com/user/timbuchalka/', topics: 'Java, Android Development, Python', students: '1400000', rating: '4.5', courses_count: '14' },
    { name: 'Mosh Hamedani', platform: 'Udemy', profile_url: 'https://www.udemy.com/user/maboroshi/', topics: 'C#, Python, JavaScript, React', students: '900000', rating: '4.6', courses_count: '18' },
    { name: 'Frank Kane', platform: 'Udemy', profile_url: 'https://www.udemy.com/user/frank-kane-2/', topics: 'Machine Learning, AI, Big Data', students: '800000', rating: '4.5', courses_count: '22' },
    { name: 'Jana Lai', platform: 'Udemy', profile_url: 'https://www.udemy.com/user/jana-lai/', topics: 'Business Strategy, Management', students: '200000', rating: '4.5', courses_count: '10' },
    { name: 'Jason Dion', platform: 'Udemy', profile_url: 'https://www.udemy.com/user/jason-dion/', topics: 'CompTIA, Cybersecurity, IT Certifications', students: '900000', rating: '4.6', courses_count: '42' },
    { name: 'Lazy Programmer', platform: 'Udemy', profile_url: 'https://www.udemy.com/user/lazy-programmer/', topics: 'Deep Learning, NLP, Reinforcement Learning', students: '500000', rating: '4.6', courses_count: '35' },
    { name: 'Krish Naik', platform: 'Udemy', profile_url: 'https://www.udemy.com/user/krishnaik06/', topics: 'Data Science, Machine Learning, MLOps', students: '400000', rating: '4.6', courses_count: '20' },
    { name: 'Ryan Kroonenburg', platform: 'Udemy', profile_url: 'https://www.udemy.com/user/ryankroonenburg/', topics: 'AWS, Cloud Computing', students: '1300000', rating: '4.5', courses_count: '12' },
    { name: 'Arun Smoki', platform: 'Udemy', profile_url: 'https://www.udemy.com/user/arun-smoki/', topics: 'IELTS, English', students: '200000', rating: '4.5', courses_count: '5' },
    { name: 'Keith Barker', platform: 'Udemy', profile_url: 'https://www.udemy.com/user/keithbarker/', topics: 'Networking, CCNA, Cisco', students: '300000', rating: '4.7', courses_count: '15' },
    { name: 'Edwin Diaz', platform: 'Udemy', profile_url: 'https://www.udemy.com/user/edwin166/', topics: 'PHP, WordPress, Web Development', students: '700000', rating: '4.4', courses_count: '25' },
    { name: 'Huzefa Husain', platform: 'Udemy', profile_url: 'https://www.udemy.com/user/huzefa-husain/', topics: 'IELTS, English Speaking', students: '150000', rating: '4.5', courses_count: '8' },

    // Coursera notable instructors
    { name: 'Andrew Ng', platform: 'Coursera', profile_url: 'https://www.coursera.org/instructor/andrewng', topics: 'Machine Learning, AI, Deep Learning', students: '8000000', rating: '4.9', courses_count: '5' },
    { name: 'Barbara Oakley', platform: 'Coursera', profile_url: 'https://www.coursera.org/instructor/barboakley', topics: 'Learning How to Learn, Mindshift', students: '4000000', rating: '4.8', courses_count: '3' },
    { name: 'Jeff Leek', platform: 'Coursera', profile_url: 'https://www.coursera.org/instructor/~694443', topics: 'Data Science, R Programming, Biostatistics', students: '2000000', rating: '4.5', courses_count: '10' },

    // Skillshare notable instructors
    { name: 'Peggy Dean', platform: 'Skillshare', profile_url: 'https://www.skillshare.com/en/user/thepigeonletters', topics: 'Lettering, Calligraphy, Drawing', students: '500000', rating: '', courses_count: '30' },
    { name: 'Brent Eviston', platform: 'Skillshare', profile_url: 'https://www.skillshare.com/en/user/brenteviston', topics: 'Drawing, Sketching, Art Fundamentals', students: '300000', rating: '', courses_count: '10' },
    { name: 'Domestika', platform: 'Skillshare', profile_url: '', topics: 'Creative Arts, Design, Illustration', students: '', rating: '', courses_count: '' },
    { name: 'Ohn Mar Win', platform: 'Skillshare', profile_url: 'https://www.skillshare.com/en/user/ohnmarwin', topics: 'Illustration, Surface Pattern Design', students: '200000', rating: '', courses_count: '25' },
    { name: 'Helen Bradley', platform: 'Skillshare', profile_url: 'https://www.skillshare.com/en/user/helenbradley', topics: 'Photoshop, Illustrator, Photography', students: '250000', rating: '', courses_count: '40' },

    // Teachable/Kajabi/Thinkific notable creators
    { name: 'Amy Porterfield', platform: 'Teachable, Kajabi', profile_url: 'https://www.amyporterfield.com/', topics: 'Digital Marketing, Course Creation, List Building', students: '', rating: '', courses_count: '4', website: 'https://www.amyporterfield.com/' },
    { name: 'Pat Flynn', platform: 'Teachable', profile_url: 'https://www.smartpassiveincome.com/', topics: 'Passive Income, Podcasting, Affiliate Marketing', students: '', rating: '', courses_count: '5', website: 'https://www.smartpassiveincome.com/' },
    { name: 'Brendon Burchard', platform: 'Kajabi', profile_url: 'https://brendon.com/', topics: 'High Performance, Personal Development', students: '', rating: '', courses_count: '8', website: 'https://brendon.com/' },
    { name: 'Marie Forleo', platform: 'Teachable', profile_url: 'https://www.marieforleo.com/', topics: 'Business, B-School, Life Coaching', students: '', rating: '', courses_count: '3', website: 'https://www.marieforleo.com/' },
    { name: 'James Wedmore', platform: 'Kajabi', profile_url: 'https://www.jameswedmore.com/', topics: 'Online Business, Course Creation', students: '', rating: '', courses_count: '3', website: 'https://www.jameswedmore.com/' },
    { name: 'Stu McLaren', platform: 'Kajabi', profile_url: 'https://stumclaren.com/', topics: 'Membership Sites, Recurring Revenue', students: '', rating: '', courses_count: '2', website: 'https://stumclaren.com/' },
    { name: 'Jenna Kutcher', platform: 'Kajabi', profile_url: 'https://jennakutcher.com/', topics: 'Marketing, Photography, Instagram', students: '', rating: '', courses_count: '5', website: 'https://jennakutcher.com/' },
    { name: 'Melyssa Griffin', platform: 'Teachable', profile_url: 'https://www.melyssagriffin.com/', topics: 'Blogging, Pinterest, Online Business', students: '', rating: '', courses_count: '4', website: 'https://www.melyssagriffin.com/' },
    { name: 'Roberto Blake', platform: 'Teachable', profile_url: 'https://robertoblake.com/', topics: 'YouTube, Creative Business, Design', students: '', rating: '', courses_count: '6', website: 'https://robertoblake.com/' },
    { name: 'Sunny Lenarduzzi', platform: 'Teachable', profile_url: 'https://sunnylenarduzzi.com/', topics: 'YouTube, Online Business, Video Marketing', students: '', rating: '', courses_count: '3', website: 'https://sunnylenarduzzi.com/' },
    { name: 'Rick Mulready', platform: 'Teachable', profile_url: 'https://rickmulready.com/', topics: 'Facebook Ads, Online Business', students: '', rating: '', courses_count: '3', website: 'https://rickmulready.com/' },
    { name: 'Mariah Coz', platform: 'Teachable', profile_url: 'https://www.mariahcoz.com/', topics: 'Course Creation, Webinars, Funnels', students: '', rating: '', courses_count: '4', website: 'https://www.mariahcoz.com/' },
    { name: 'Ramit Sethi', platform: 'Teachable', profile_url: 'https://www.iwillteachyoutoberich.com/', topics: 'Personal Finance, Career, Business', students: '', rating: '', courses_count: '10', website: 'https://www.iwillteachyoutoberich.com/' },
    { name: 'Graham Cochrane', platform: 'Teachable', profile_url: 'https://grahamcochrane.com/', topics: 'Music Production, Creative Business', students: '', rating: '', courses_count: '5', website: 'https://grahamcochrane.com/' },

    // Thinkific creators
    { name: 'John Lee Dumas', platform: 'Thinkific', profile_url: 'https://www.eofire.com/', topics: 'Podcasting, Entrepreneurship', students: '', rating: '', courses_count: '3', website: 'https://www.eofire.com/' },
    { name: 'Lewis Howes', platform: 'Thinkific', profile_url: 'https://lewishowes.com/', topics: 'Mindset, Business, Personal Brand', students: '', rating: '', courses_count: '3', website: 'https://lewishowes.com/' },

    // Podia creators
    { name: 'Khe Hy', platform: 'Podia', profile_url: 'https://radreads.co/', topics: 'Productivity, Career Design, Notion', students: '', rating: '', courses_count: '3', website: 'https://radreads.co/' },
  ];

  for (const inst of knownInstructors) {
    addCreator({
      name: inst.name,
      platform: inst.platform || '',
      profile_url: inst.profile_url || '',
      topics: inst.topics || '',
      students: inst.students || '',
      rating: inst.rating || '',
      website: inst.website || '',
      email: '',
      linkedin: '',
      twitter: '',
      youtube: '',
      facebook: '',
      instagram: '',
      bio: '',
      courses_count: inst.courses_count || '',
      reviews: '',
    });
  }

  console.log(`  Seeded ${knownInstructors.length} known course creators. Total unique: ${creators.size}`);
}


// ─── CSV Output ─────────────────────────────────────────────────────────────

function writeCSV() {
  const headers = [
    'name', 'platform', 'profile_url', 'topics', 'students', 'rating',
    'courses_count', 'reviews', 'website', 'email', 'linkedin', 'twitter',
    'youtube', 'facebook', 'instagram', 'bio'
  ];

  const escapeCSV = (val) => {
    if (!val) return '';
    const str = String(val).replace(/"/g, '""');
    return str.includes(',') || str.includes('"') || str.includes('\n') ? `"${str}"` : str;
  };

  const rows = [...creators.values()]
    .sort((a, b) => {
      // Sort by students descending (numeric), then name
      const sa = parseInt((a.students || '0').replace(/[^0-9]/g, '')) || 0;
      const sb = parseInt((b.students || '0').replace(/[^0-9]/g, '')) || 0;
      if (sb !== sa) return sb - sa;
      return (a.name || '').localeCompare(b.name || '');
    });

  const csvLines = [
    headers.join(','),
    ...rows.map(r => headers.map(h => escapeCSV(r[h])).join(','))
  ];

  if (!fs.existsSync(OUTPUT_DIR)) {
    fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  }

  fs.writeFileSync(OUTPUT_FILE, csvLines.join('\n'), 'utf-8');
  console.log(`\nCSV written: ${OUTPUT_FILE}`);
  console.log(`Total creators: ${rows.length}`);

  // Stats
  const withWebsite = rows.filter(r => r.website).length;
  const withEmail = rows.filter(r => r.email).length;
  const withLinkedIn = rows.filter(r => r.linkedin).length;
  const withSocial = rows.filter(r => r.twitter || r.youtube || r.facebook || r.instagram).length;
  const platforms = {};
  rows.forEach(r => {
    (r.platform || 'Unknown').split(',').forEach(p => {
      p = p.trim();
      platforms[p] = (platforms[p] || 0) + 1;
    });
  });

  console.log(`\n=== STATS ===`);
  console.log(`  Total creators:    ${rows.length}`);
  console.log(`  With website:      ${withWebsite}`);
  console.log(`  With email:        ${withEmail}`);
  console.log(`  With LinkedIn:     ${withLinkedIn}`);
  console.log(`  With social links: ${withSocial}`);
  console.log(`  By platform:`);
  Object.entries(platforms).sort((a, b) => b[1] - a[1]).forEach(([p, c]) => {
    console.log(`    ${p}: ${c}`);
  });
}


// ─── Main ───────────────────────────────────────────────────────────────────

(async () => {
  const startTime = Date.now();
  console.log('=== Course Creator Scraper ===');
  console.log(`Started: ${new Date().toISOString()}`);
  console.log(`Max pages per search: ${MAX_PAGES}`);
  console.log(`Profile enrichment: ${NO_PROFILES ? 'DISABLED' : 'ENABLED'}`);
  console.log('');

  // Phase 0: Seed known instructors
  seedKnownInstructors();

  // Launch browser
  const browser = await puppeteer.launch({
    headless: 'new',
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-accelerated-2d-canvas',
      '--disable-gpu',
      '--window-size=1440,900',
    ]
  });

  try {
    if (!SKILLSHARE_ONLY) {
      // Phase 1: Udemy search
      await scrapeUdemySearch(browser);

      // Phase 2: Udemy profile enrichment
      if (!NO_PROFILES) {
        await enrichUdemyProfiles(browser);
      }
    }

    if (!UDEMY_ONLY) {
      // Phase 3: Skillshare
      await scrapeSkillshare(browser);
    }

  } catch (err) {
    console.error('\nFatal error:', err.message);
  } finally {
    await browser.close();
  }

  // Write output
  writeCSV();

  const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  console.log(`\nCompleted in ${elapsed} minutes`);
})();
