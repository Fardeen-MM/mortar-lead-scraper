/**
 * AI Module — Claude API integration for lead intelligence
 * Zero dependencies, raw fetch. Haiku for speed, Sonnet for depth.
 */

const API_URL = 'https://api.anthropic.com/v1/messages';
const HAIKU = 'claude-haiku-4-5-20251001';
const SONNET = 'claude-sonnet-4-5-20250929';

// Load .env if present
const fs = require('fs');
const path = require('path');
const envPath = path.join(__dirname, '..', '.env');
if (fs.existsSync(envPath)) {
  for (const line of fs.readFileSync(envPath, 'utf-8').split('\n')) {
    const m = line.match(/^\s*([^#=]+?)\s*=\s*(.*?)\s*$/);
    if (m && !process.env[m[1]]) process.env[m[1]] = m[2];
  }
}

function getApiKey() {
  const key = process.env.ANTHROPIC_API_KEY;
  if (!key) throw new Error('ANTHROPIC_API_KEY not set');
  return key;
}

/**
 * Call Claude API with retry logic
 */
async function callClaude({ model, system, prompt, maxTokens = 2000, temperature = 0.3 }) {
  const apiKey = getApiKey();
  const body = {
    model,
    max_tokens: maxTokens,
    temperature,
    system,
    messages: [{ role: 'user', content: prompt }],
  };

  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const res = await fetch(API_URL, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-api-key': apiKey,
          'anthropic-version': '2023-06-01',
        },
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(60000),
      });

      if (res.status === 429 || res.status === 529) {
        const wait = Math.pow(2, attempt + 1) * 1000;
        console.log(`[AI] Rate limited (${res.status}), retrying in ${wait}ms...`);
        await new Promise(r => setTimeout(r, wait));
        continue;
      }

      if (!res.ok) {
        const errText = await res.text().catch(() => '');
        if (res.status === 400 && errText.includes('credit balance')) {
          throw new Error('API credits depleted — add credits at console.anthropic.com');
        }
        if (res.status === 401) {
          throw new Error('Invalid API key — check ANTHROPIC_API_KEY in .env');
        }
        throw new Error(`Claude API ${res.status}: ${errText.substring(0, 200)}`);
      }

      const data = await res.json();
      return data.content?.[0]?.text || '';
    } catch (err) {
      if (attempt < 2 && (err.name === 'TimeoutError' || err.message.includes('fetch failed'))) {
        console.log(`[AI] Request failed (${err.message}), retry ${attempt + 1}/3...`);
        await new Promise(r => setTimeout(r, 2000));
        continue;
      }
      throw err;
    }
  }
  throw new Error('Claude API: max retries exceeded');
}

/**
 * Parse JSON from Claude response (handles markdown fences, embedded JSON)
 */
function parseJSON(text) {
  // Direct parse
  try { return JSON.parse(text.trim()); } catch {}
  // Extract from ```json ... ```
  const fenced = text.match(/```(?:json)?\s*\n?([\s\S]*?)\n?```/);
  if (fenced) try { return JSON.parse(fenced[1].trim()); } catch {}
  // Find JSON object/array
  const objMatch = text.match(/\{[\s\S]*\}/);
  if (objMatch) try { return JSON.parse(objMatch[0]); } catch {}
  const arrMatch = text.match(/\[[\s\S]*\]/);
  if (arrMatch) try { return JSON.parse(arrMatch[0]); } catch {}
  throw new Error('Could not parse JSON from AI response');
}

// ============================================================
// AI FEATURES
// ============================================================

const LEAD_SCHEMA = `
The leads SQLite table has these columns:
- id (INTEGER PRIMARY KEY)
- first_name, last_name, firm_name, city, state, country
- phone, email, website, linkedin_url
- bar_number, bar_status, admission_date
- practice_area, title, bio, education
- source, profile_url
- lead_score (0-100), pipeline_stage (new/contacted/qualified/proposal/won/lost)
- tags (comma-separated), owner, notes
- created_at, updated_at, last_enriched_at
- enrichment_attempts, last_enrichment_error
- email_source, email_verified
States use codes: FL, CA, NY, OH, TX, PA, etc. Canadian provinces: CA-AB, CA-BC. UK: UK-SC, UK-EW-BAR. Australia: AU-NSW, AU-QLD, etc.
Countries: US, CA, UK, AU, FR, IE, IT, NZ, SG, HK.
`;

/**
 * AI Ask Brain — Natural language query → SQL → results
 */
async function askBrain(question, db) {
  const system = `You are a SQL expert for a lawyer lead database. Convert natural language questions into SQLite queries.
${LEAD_SCHEMA}
Rules:
- ALWAYS return valid JSON with keys: "sql" (the SELECT query), "explanation" (1 sentence about what you're querying)
- ONLY generate SELECT queries. Never INSERT/UPDATE/DELETE/DROP.
- Use LIKE for text searches (case-insensitive).
- For "best" or "top" leads, ORDER BY lead_score DESC.
- For "gold" leads, WHERE email IS NOT NULL AND email != '' AND phone IS NOT NULL AND phone != ''.
- For "contactable", WHERE (email IS NOT NULL AND email != '') OR (phone IS NOT NULL AND phone != '').
- LIMIT results to 50 unless the user asks for counts/aggregates.
- For counts, use COUNT(*), GROUP BY as needed.
- Return raw JSON only, no markdown fences.`;

  const text = await callClaude({
    model: HAIKU,
    system,
    prompt: question,
    maxTokens: 500,
    temperature: 0.1,
  });

  const parsed = parseJSON(text);
  if (!parsed.sql) throw new Error('AI did not generate a SQL query');

  // Safety: only allow SELECT
  const sqlUpper = parsed.sql.trim().toUpperCase();
  if (!sqlUpper.startsWith('SELECT')) throw new Error('AI generated non-SELECT query — blocked');
  if (/\b(DROP|DELETE|UPDATE|INSERT|ALTER|CREATE|TRUNCATE)\b/.test(sqlUpper)) {
    throw new Error('AI generated destructive query — blocked');
  }

  try {
    const results = db.prepare(parsed.sql).all();
    return {
      sql: parsed.sql,
      explanation: parsed.explanation || '',
      results: results.slice(0, 200),
      count: results.length,
    };
  } catch (err) {
    throw new Error(`SQL Error: ${err.message} | Query: ${parsed.sql}`);
  }
}

/**
 * AI Lead Insights — Deep analysis of a single lead
 */
async function analyzeLeadInsights(lead) {
  const system = `You are a sales intelligence analyst for a legal marketing agency called Mortar Metrics. Analyze attorney leads and provide actionable insights for cold outreach.
Return JSON with these keys:
- "quality_assessment" (string: 1-2 sentences on data quality and completeness)
- "outreach_angle" (string: the best angle to approach this attorney, based on their practice area, firm, location)
- "personalized_opener" (string: a compelling 1-2 sentence email opener personalized to this attorney)
- "pain_points" (array of 2-3 strings: likely marketing pain points for their practice type)
- "recommended_services" (array of 2-3 strings: Mortar Metrics services that would help them)
- "priority" (string: "high", "medium", or "low" — how good a prospect they are)
- "next_steps" (array of 2-3 strings: concrete next actions)
Return raw JSON only.`;

  const profile = [
    `Name: ${lead.first_name} ${lead.last_name}`,
    lead.firm_name ? `Firm: ${lead.firm_name}` : null,
    lead.title ? `Title: ${lead.title}` : null,
    `Location: ${lead.city || '?'}, ${lead.state || '?'}, ${lead.country || 'US'}`,
    lead.practice_area ? `Practice Area: ${lead.practice_area}` : null,
    lead.email ? `Email: ${lead.email}` : 'No email on file',
    lead.phone ? `Phone: ${lead.phone}` : 'No phone on file',
    lead.website ? `Website: ${lead.website}` : 'No website on file',
    lead.linkedin_url ? `LinkedIn: ${lead.linkedin_url}` : null,
    lead.bio ? `Bio: ${lead.bio.substring(0, 500)}` : null,
    lead.education ? `Education: ${lead.education}` : null,
    lead.admission_date ? `Admitted: ${lead.admission_date}` : null,
    lead.bar_status ? `Bar Status: ${lead.bar_status}` : null,
    `Lead Score: ${lead.lead_score || 0}/100`,
    lead.tags ? `Tags: ${lead.tags}` : null,
  ].filter(Boolean).join('\n');

  const text = await callClaude({
    model: HAIKU,
    system,
    prompt: `Analyze this attorney lead:\n\n${profile}`,
    maxTokens: 800,
    temperature: 0.4,
  });

  return parseJSON(text);
}

/**
 * AI Email Writer — Generate personalized cold email
 */
async function writeEmail(lead, options = {}) {
  const tone = options.tone || 'professional-casual';
  const goal = options.goal || 'book a discovery call';
  const template = options.template || 'intro';

  const system = `You are an expert cold email copywriter for Mortar Metrics, a legal marketing agency. Write emails that attorneys actually respond to.

Rules:
- Keep it SHORT (3-5 sentences max for body, not counting subject)
- Sound human, not salesy. No "I hope this finds you well."
- Lead with value or a specific observation about THEIR practice
- One clear CTA (usually booking a call)
- Tone: ${tone}
- Goal: ${goal}

Return JSON with these keys:
- "subject" (string: compelling subject line, under 50 chars)
- "body" (string: the email body, use \\n for line breaks)
- "ps" (string or null: optional P.S. line for social proof or urgency)
- "follow_up" (string: a 2-sentence follow-up email for 3 days later)
Return raw JSON only.`;

  const context = [
    `Name: ${lead.first_name} ${lead.last_name}`,
    lead.firm_name ? `Firm: ${lead.firm_name}` : null,
    `Location: ${lead.city || '?'}, ${lead.state || '?'}`,
    lead.practice_area ? `Practice Area: ${lead.practice_area}` : null,
    lead.website ? `Website: ${lead.website}` : null,
    lead.bio ? `Bio: ${lead.bio.substring(0, 300)}` : null,
    lead.title ? `Title: ${lead.title}` : null,
    `Template: ${template}`,
  ].filter(Boolean).join('\n');

  const text = await callClaude({
    model: SONNET,
    system,
    prompt: `Write a cold email for this attorney lead:\n\n${context}`,
    maxTokens: 600,
    temperature: 0.6,
  });

  return parseJSON(text);
}

/**
 * AI Practice Area Classifier — Auto-tag leads with practice areas
 */
async function classifyPracticeAreas(leads) {
  const STANDARD_AREAS = [
    'Personal Injury', 'Family Law', 'Criminal Defense', 'Corporate/Business',
    'Real Estate', 'Estate Planning', 'Bankruptcy', 'Immigration',
    'Employment/Labor', 'Intellectual Property', 'Tax', 'Environmental',
    'Healthcare', 'Civil Litigation', 'Insurance', 'DUI/DWI',
    'Workers Compensation', 'Medical Malpractice', 'Class Action',
    'Government/Administrative', 'Elder Law', 'Trusts & Estates',
  ];

  const system = `You classify attorneys into practice areas based on their name, firm name, title, bio, and location context.

Standard practice areas: ${STANDARD_AREAS.join(', ')}

Rules:
- Return JSON array of objects with "id" (lead ID) and "practice_area" (best match from standard list)
- If you can't determine practice area, use "General Practice"
- Use firm name clues (e.g. "Smith Injury Law" → "Personal Injury")
- Use title clues (e.g. "Criminal Defense Attorney" → "Criminal Defense")
- One practice area per lead (the primary one)
Return raw JSON array only.`;

  // Process in batches of 20
  const results = [];
  for (let i = 0; i < leads.length; i += 20) {
    const batch = leads.slice(i, i + 20);
    const prompt = batch.map(l =>
      `ID:${l.id} | ${l.first_name} ${l.last_name} | Firm: ${l.firm_name || '?'} | Title: ${l.title || '?'} | Bio: ${(l.bio || '').substring(0, 100)} | City: ${l.city || '?'}, ${l.state || '?'}`
    ).join('\n');

    const text = await callClaude({
      model: HAIKU,
      system,
      prompt: `Classify these attorneys:\n${prompt}`,
      maxTokens: 1000,
      temperature: 0.1,
    });

    try {
      const parsed = parseJSON(text);
      results.push(...(Array.isArray(parsed) ? parsed : []));
    } catch (err) {
      console.error(`[AI] Practice area batch ${i} parse error:`, err.message);
    }
  }
  return results;
}

/**
 * AI Data Quality Auditor — Find issues across leads
 */
async function auditDataQuality(leads) {
  const system = `You are a data quality auditor for an attorney lead database. Analyze a batch of leads and identify data quality issues.

Return JSON with these keys:
- "issues" (array of objects, each with: "id" (lead ID), "field" (which field has the issue), "issue" (description), "severity" ("high"/"medium"/"low"), "suggestion" (how to fix))
- "summary" (string: 1-2 sentence overview of data quality)
- "score" (number 0-100: overall quality score for this batch)

Common issues to check:
- Missing critical fields (no email AND no phone = low quality)
- Name formatting (all caps, missing first/last, numbers in name)
- Invalid-looking emails (test@, noreply@, info@)
- Phone format issues
- Mismatched state/city combinations
- Firm name issues (too short, generic like "Law Office")
- Duplicate-looking entries (same name, different IDs)
Return raw JSON only.`;

  const prompt = leads.map(l =>
    `ID:${l.id} | ${l.first_name} ${l.last_name} | Firm: ${l.firm_name || ''} | ${l.city || ''}, ${l.state || ''} | Email: ${l.email || ''} | Phone: ${l.phone || ''} | Score: ${l.lead_score || 0}`
  ).join('\n');

  const text = await callClaude({
    model: HAIKU,
    system,
    prompt: `Audit these leads for data quality:\n${prompt}`,
    maxTokens: 1500,
    temperature: 0.1,
  });

  return parseJSON(text);
}

/**
 * AI Dashboard Intelligence — Generate daily briefing
 */
async function generateDashboardBrief(stats) {
  const system = `You are a sales intelligence analyst. Generate a crisp daily briefing about a lawyer lead database for a legal marketing agency.

Return JSON with these keys:
- "headline" (string: one punchy line summarizing the database state, like "13.5K leads, 30% contactable — time to enrich")
- "highlights" (array of 3-4 strings: key positive metrics)
- "opportunities" (array of 2-3 strings: specific actionable opportunities)
- "warnings" (array of 1-2 strings: data issues or risks to address)
- "recommendation" (string: single most impactful action to take today)
Return raw JSON only.`;

  const prompt = `Lead Database Stats:
- Total leads: ${stats.total}
- With email: ${stats.withEmail} (${stats.coverage?.email || 0}%)
- With phone: ${stats.withPhone} (${stats.coverage?.phone || 0}%)
- With website: ${stats.withWebsite}
- Gold leads (email+phone): ${stats.gold || 0}
- Contactable: ${stats.contactable || 0}
- Enriched: ${stats.enriched || 0}
- Score distribution: Excellent(80+): ${stats.scoreDistribution?.excellent || 0}, Good(60-79): ${stats.scoreDistribution?.good || 0}, Fair(40-59): ${stats.scoreDistribution?.fair || 0}, Poor(<40): ${stats.scoreDistribution?.poor || 0}
- States covered: ${(stats.byState || []).length}
- Countries: ${(stats.byCountry || []).map(c => c.country).join(', ')}
- Top states: ${(stats.byState || []).slice(0, 5).map(s => `${s.state}(${s.count})`).join(', ')}
- Enriched last 24h: ${stats.enrichedLast24h || 0}
- Unique firms: ${stats.uniqueFirms || 0}`;

  const text = await callClaude({
    model: HAIKU,
    system,
    prompt,
    maxTokens: 600,
    temperature: 0.4,
  });

  return parseJSON(text);
}

/**
 * AI Summarize Lead Batch — Quick summary of search results
 */
async function summarizeBatch(leads, query) {
  const system = `You summarize a batch of attorney leads in 2-3 sentences. Be specific about patterns (most common practice areas, locations, data quality). Keep it concise and actionable.`;

  const prompt = `Query: "${query || 'all leads'}"
Results: ${leads.length} leads
Sample: ${leads.slice(0, 20).map(l => `${l.first_name} ${l.last_name} (${l.firm_name || '?'}, ${l.city || '?'} ${l.state || '?'}) ${l.practice_area || ''}`).join('; ')}
States: ${[...new Set(leads.map(l => l.state).filter(Boolean))].join(', ')}
With email: ${leads.filter(l => l.email).length}/${leads.length}
With phone: ${leads.filter(l => l.phone).length}/${leads.length}`;

  return await callClaude({
    model: HAIKU,
    system,
    prompt,
    maxTokens: 200,
    temperature: 0.3,
  });
}

/**
 * Explain a lead's score — why it's high/low, what would improve it
 */
async function explainScore(lead) {
  const system = `You explain why a lawyer/attorney lead has a particular score. Be specific about what data is present/missing and how that affects the score. Give 2-3 actionable suggestions to improve it. Respond in JSON: { "grade": "A/B/C/D/F", "explanation": "...", "strengths": ["..."], "gaps": ["..."], "suggestions": ["..."] }`;

  const fields = {
    name: `${lead.first_name} ${lead.last_name}`,
    firm: lead.firm_name || 'missing',
    city: lead.city || 'missing',
    state: lead.state || 'missing',
    email: lead.email ? 'present' : 'missing',
    phone: lead.phone ? 'present' : 'missing',
    website: lead.website ? 'present' : 'missing',
    bar_number: lead.bar_number ? 'present' : 'missing',
    practice_area: lead.practice_area || 'missing',
    title: lead.title || 'missing',
    linkedin: lead.linkedin_url ? 'present' : 'missing',
    education: lead.education ? 'present' : 'missing',
  };

  const prompt = `Lead score: ${lead.lead_score || 0}/100
Fields: ${JSON.stringify(fields)}
Email verified: ${lead.email_verified === 1 ? 'yes' : lead.email_verified === -1 ? 'invalid' : 'unknown'}
Source: ${lead.primary_source || 'unknown'}`;

  const result = await callClaude({
    model: HAIKU,
    system,
    prompt,
    maxTokens: 400,
    temperature: 0.2,
  });

  return parseJSON(result);
}

/**
 * Generate outreach talking points for a lead
 */
async function generateTalkingPoints(lead) {
  const system = `You generate 3-5 personalized talking points for reaching out to a lawyer/attorney. Base them on their practice area, location, firm size, and any available data. Each point should be a single sentence. Respond in JSON: { "points": ["..."], "icebreaker": "...", "cta": "..." }`;

  const prompt = `Attorney: ${lead.first_name} ${lead.last_name}
Firm: ${lead.firm_name || 'unknown'}
Location: ${lead.city || '?'}, ${lead.state || '?'}
Practice: ${lead.practice_area || 'unknown'}
Title: ${lead.title || 'unknown'}
Website: ${lead.website || 'none'}`;

  const result = await callClaude({
    model: HAIKU,
    system,
    prompt,
    maxTokens: 400,
    temperature: 0.5,
  });

  return parseJSON(result);
}

/**
 * AI Reply Analyzer — Categorize and extract insights from email replies
 */
async function analyzeReply(replyText, leadContext = {}) {
  const system = `You analyze email replies from prospects (attorneys/lawyers). Extract the interest level, sentiment, key topics, objections, and recommend the best next action.

Return JSON:
{
  "interest": "high|medium|low|none",
  "sentiment": "positive|neutral|negative|mixed",
  "category": "interested|pricing_question|schedule_call|not_now|not_interested|out_of_office|referral|info_request|objection|unsubscribe",
  "summary": "1-sentence summary of what they said",
  "pain_points": ["array of pain points or needs mentioned"],
  "objections": ["array of objections raised"],
  "next_action": "specific recommended next step",
  "urgency": "high|medium|low",
  "suggested_reply_tone": "brief description of ideal response tone"
}`;

  const ctx = leadContext.first_name
    ? `\nLead context: ${leadContext.first_name} ${leadContext.last_name || ''}, ${leadContext.firm_name || 'Unknown firm'}, ${leadContext.practice_area || 'Unknown practice'}, ${leadContext.city || ''} ${leadContext.state || ''}`
    : '';

  const prompt = `Analyze this email reply:${ctx}

---
${replyText.slice(0, 2000)}
---`;

  const result = await callClaude({
    model: HAIKU,
    system,
    prompt,
    maxTokens: 500,
    temperature: 0.2,
  });

  return parseJSON(result);
}

/**
 * AI Cold Call Script Generator — Personalized call scripts with objection handling
 */
async function generateCallScript(lead, options = {}) {
  const system = `You write cold call scripts for a legal marketing agency reaching out to attorneys. The script should feel natural, not robotic. Include an opener, value proposition, discovery questions, and closing.

Return JSON:
{
  "opener": "15-second opening line (name + reason for call)",
  "value_prop": "20-second value statement tailored to their practice area",
  "discovery_questions": ["3-4 open-ended questions to uncover pain points"],
  "objection_responses": {
    "too_busy": "response if they say they're too busy",
    "not_interested": "response if they say not interested",
    "have_agency": "response if they already have a marketing agency",
    "too_expensive": "response if they mention cost concerns",
    "send_info": "response if they say just send me information"
  },
  "closing": "ask for specific next step (meeting, demo, etc.)",
  "voicemail": "30-second voicemail script if no answer"
}`;

  const fields = {
    name: `${lead.first_name || ''} ${lead.last_name || ''}`.trim(),
    firm: lead.firm_name || 'their firm',
    practice: lead.practice_area || 'general practice',
    city: lead.city || '',
    state: lead.state || '',
    title: lead.title || '',
  };

  const extra = options.context || '';
  const prompt = `Generate a cold call script for:
Name: ${fields.name}
Firm: ${fields.firm}
Practice Area: ${fields.practice}
Location: ${fields.city}, ${fields.state}
Title: ${fields.title}
${extra ? `Additional context: ${extra}` : ''}`;

  const result = await callClaude({
    model: HAIKU,
    system,
    prompt,
    maxTokens: 800,
    temperature: 0.5,
  });

  return parseJSON(result);
}

/**
 * AI Win Probability — Predict likelihood of conversion based on lead data + engagement
 */
async function scoreWinProbability(lead, engagementData = {}) {
  const system = `You are a sales analytics AI. Given a lead's profile and engagement history, estimate the probability of converting them into a client. Consider: data completeness, engagement recency, practice area demand, firm size signals, geographic market.

Return JSON:
{
  "probability": 0-100,
  "confidence": "high|medium|low",
  "factors_positive": ["up to 3 factors increasing probability"],
  "factors_negative": ["up to 3 factors decreasing probability"],
  "recommended_channel": "email|phone|linkedin|mail",
  "best_time": "suggested best outreach timing",
  "tier": "hot|warm|cool|cold"
}`;

  const profile = {
    name: `${lead.first_name || ''} ${lead.last_name || ''}`.trim(),
    firm: lead.firm_name || '',
    practice: lead.practice_area || '',
    city: lead.city || '',
    state: lead.state || '',
    has_email: !!lead.email,
    has_phone: !!lead.phone,
    has_website: !!lead.website,
    has_linkedin: !!lead.linkedin_url,
    score: lead.score || 0,
    title: lead.title || '',
  };

  const engagement = {
    total_contacts: engagementData.total || 0,
    last_contact: engagementData.lastContact || 'never',
    channels_used: (engagementData.channels || []).map(c => c.channel).join(', ') || 'none',
    replies_received: engagementData.replies || 0,
  };

  const prompt = `Estimate win probability for:
Profile: ${JSON.stringify(profile)}
Engagement: ${JSON.stringify(engagement)}`;

  const result = await callClaude({
    model: HAIKU,
    system,
    prompt,
    maxTokens: 400,
    temperature: 0.2,
  });

  return parseJSON(result);
}

/**
 * AI Sequence Email Variants — Generate A/B variants for outreach sequences
 */
async function generateSequenceVariants(stepContext, numVariants = 3) {
  const system = `You write email outreach variants for a legal marketing agency. Each variant should have a different angle/approach but same core message. Keep emails short (3-5 sentences max for cold emails, 2-3 for follow-ups).

Return JSON:
{
  "variants": [
    {
      "label": "Variant A — [brief description]",
      "subject": "email subject line",
      "body": "email body text"
    }
  ]
}`;

  const prompt = `Generate ${numVariants} email variants for:
Step: ${stepContext.stepNumber || 1} of sequence
Channel: ${stepContext.channel || 'email'}
Purpose: ${stepContext.purpose || 'initial cold outreach'}
Practice area target: ${stepContext.practiceArea || 'general'}
Previous step context: ${stepContext.previousStep || 'none (first touch)'}
Tone: ${stepContext.tone || 'professional but friendly'}`;

  const result = await callClaude({
    model: HAIKU,
    system,
    prompt,
    maxTokens: 1000,
    temperature: 0.7,
  });

  return parseJSON(result);
}

/**
 * AI Outreach Planner — Creates a prioritized outreach plan for a set of leads
 */
async function planOutreach(leads, options = {}) {
  const system = `You are an outreach planning AI for a legal marketing agency. Given a set of attorney leads, create a prioritized 5-day outreach plan. Consider: lead score, data completeness, practice area demand, geographic clustering.

Return JSON:
{
  "summary": "1-2 sentence overview of the plan",
  "daily_plan": [
    {
      "day": "Day 1",
      "focus": "what to focus on this day",
      "leads": [
        {
          "name": "First Last",
          "action": "email|call|linkedin",
          "reason": "why this lead, why this channel",
          "priority": "high|medium"
        }
      ]
    }
  ],
  "tips": ["2-3 tactical tips for this specific batch"]
}`;

  const leadSummaries = leads.slice(0, 20).map(l => ({
    name: `${l.first_name || ''} ${l.last_name || ''}`.trim(),
    firm: l.firm_name || '',
    practice: l.practice_area || '',
    city: l.city || '',
    state: l.state || '',
    score: l.lead_score || 0,
    has_email: !!l.email,
    has_phone: !!l.phone,
    has_linkedin: !!l.linkedin_url,
  }));

  const prompt = `Create a 5-day outreach plan for these ${leads.length} leads:
${JSON.stringify(leadSummaries, null, 1)}
${options.goal ? `Goal: ${options.goal}` : ''}`;

  const result = await callClaude({
    model: HAIKU,
    system,
    prompt,
    maxTokens: 1200,
    temperature: 0.4,
  });

  return parseJSON(result);
}

/**
 * Analyze practice area market intelligence
 */
async function analyzePracticeAreaMarket(stats) {
  const system = `You are a legal market intelligence analyst. Given data about practice area distribution in a lead database, provide actionable market insights.

Return JSON:
{
  "market_overview": "2-3 sentence overview of the market",
  "top_opportunities": [
    {
      "practice_area": "name",
      "opportunity": "why this is a good target",
      "lead_quality": "high|medium|low",
      "competition_level": "high|medium|low",
      "recommended_action": "specific next step"
    }
  ],
  "underserved_areas": ["practice areas with few leads but high demand"],
  "geographic_insights": ["key geographic patterns"],
  "recommendations": ["3-5 strategic recommendations"]
}`;

  const prompt = `Analyze this legal market data:
${JSON.stringify(stats, null, 1)}

Identify the best practice area opportunities, underserved markets, and strategic recommendations for a legal marketing agency.`;

  const result = await callClaude({
    model: HAIKU,
    system,
    prompt,
    maxTokens: 1000,
    temperature: 0.3,
  });

  return parseJSON(result);
}

/**
 * Generate data quality fix suggestions for a batch of leads
 */
async function suggestDataFixes(sampleLeads) {
  const system = `You are a data quality specialist. Analyze attorney lead records and suggest specific fixes. Check for: malformed names (all caps, initials only), missing critical fields, inconsistent formatting, suspicious data patterns.

Return JSON:
{
  "issues_found": number,
  "fixes": [
    {
      "lead_id": number,
      "field": "field name",
      "current_value": "current",
      "suggested_fix": "what to change",
      "reason": "why",
      "confidence": "high|medium|low"
    }
  ],
  "patterns": ["common data quality patterns across this batch"],
  "auto_fixable": number
}`;

  const prompt = `Review these ${sampleLeads.length} lead records for data quality issues:
${JSON.stringify(sampleLeads.slice(0, 15).map(l => ({
    id: l.id, first_name: l.first_name, last_name: l.last_name, email: l.email,
    phone: l.phone, firm_name: l.firm_name, city: l.city, state: l.state,
    website: l.website, practice_area: l.practice_area, bar_status: l.bar_status,
  })), null, 1)}`;

  const result = await callClaude({
    model: HAIKU,
    system,
    prompt,
    maxTokens: 1000,
    temperature: 0.2,
  });

  return parseJSON(result);
}

/**
 * Check if AI is available (API key configured)
 */
function isAvailable() {
  return !!process.env.ANTHROPIC_API_KEY;
}

module.exports = {
  callClaude,
  parseJSON,
  askBrain,
  analyzeLeadInsights,
  writeEmail,
  classifyPracticeAreas,
  auditDataQuality,
  generateDashboardBrief,
  summarizeBatch,
  explainScore,
  generateTalkingPoints,
  analyzeReply,
  generateCallScript,
  scoreWinProbability,
  generateSequenceVariants,
  planOutreach,
  analyzePracticeAreaMarket,
  suggestDataFixes,
  isAvailable,
  HAIKU,
  SONNET,
};
