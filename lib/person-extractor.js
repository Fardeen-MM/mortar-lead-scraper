/**
 * Person Extractor — extract individual people from business websites
 *
 * Given a business website, finds people (staff, team members, partners)
 * by analyzing team/about pages for names, titles, emails, and phones.
 *
 * Extraction strategies (tried in order):
 *   1. JSON-LD / Microdata — structured Person schema markup
 *   2. Card patterns — .team-member, .staff-card, .person, .bio-card selectors
 *   3. Heading heuristic — <h2>/<h3>/<h4> with validated human names + nearby title
 *   4. Image alt text — <img alt="John Smith"> in card containers with title context
 *
 * Name validation uses a 300+ common first names dictionary to distinguish
 * real human names from UI elements like "Quick Links" or "Health Care".
 */

const puppeteer = require('puppeteer');
const { log } = require('./logger');
const { RateLimiter, sleep } = require('./rate-limiter');

// Common team/about page paths — ordered by likelihood
// Expanded 2026-04-16 per audit (was 10 paths, now 35+ covering multiple niches).
const TEAM_PATHS = [
  // Most common
  '/about', '/about-us', '/team', '/our-team', '/staff', '/people',
  '/our-people', '/meet-the-team', '/meet-our-team', '/leadership',

  // Legal / immigration
  '/attorneys', '/lawyers', '/partners', '/associates', '/advocates',
  '/barristers', '/solicitors', '/counsel', '/firm', '/our-firm',
  '/our-attorneys', '/consultants', '/immigration-consultants',

  // Business / corporate
  '/who-we-are', '/company/team', '/company/about', '/about/team',
  '/about/people', '/about/leadership', '/about-us/team', '/our-story',
  '/founders', '/executives', '/management',

  // Healthcare / education
  '/providers', '/doctors', '/physicians', '/faculty', '/instructors',
  '/staff-directory', '/our-staff', '/directory',

  // International variations
  '/en/team', '/en/about', '/en/people', '/equipe', '/equipo', '/persone',

  // Contact fallback
  '/contact', '/contact-us', '/contact/team', '/get-in-touch',
];

// 300+ most common English first names (male + female)
// Used to validate that extracted text is actually a person's name
const COMMON_FIRST_NAMES = new Set([
  // Male
  'james','robert','john','michael','david','william','richard','joseph','thomas','charles',
  'christopher','daniel','matthew','anthony','mark','donald','steven','paul','andrew','joshua',
  'kenneth','kevin','brian','george','timothy','ronald','edward','jason','jeffrey','ryan',
  'jacob','gary','nicholas','eric','jonathan','stephen','larry','justin','scott','brandon',
  'benjamin','samuel','raymond','gregory','frank','alexander','patrick','jack','dennis','jerry',
  'tyler','aaron','jose','adam','nathan','henry','peter','zachary','douglas','harold',
  'kyle','noah','carl','gerald','keith','roger','arthur','terry','sean','austin',
  'christian','albert','joe','ethan','jesse','ralph','roy','louis','eugene','philip',
  'russell','bobby','harry','vincent','bruce','dylan','willie','jordan','alan','billy',
  'howard','wayne','elijah','randy','gabriel','mason','logan','johnny','walter','connor',
  // Female
  'mary','patricia','jennifer','linda','barbara','elizabeth','susan','jessica','sarah','karen',
  'lisa','nancy','betty','margaret','sandra','ashley','dorothy','kimberly','emily','donna',
  'michelle','carol','amanda','melissa','deborah','stephanie','rebecca','sharon','laura','cynthia',
  'kathleen','amy','angela','shirley','anna','brenda','pamela','emma','nicole','helen','samantha',
  'katherine','christine','debra','rachel','carolyn','janet','catherine','maria','heather','diane',
  'ruth','julie','olivia','joyce','virginia','victoria','kelly','lauren','christina','joan',
  'evelyn','judith','megan','andrea','cheryl','hannah','jacqueline','martha','gloria','teresa',
  'ann','sara','madison','frances','kathryn','janice','jean','abigail','alice','judy',
  'sophia','grace','denise','amber','doris','marilyn','danielle','beverly','isabella','theresa',
  'diana','natalie','brittany','charlotte','marie','kayla','alexis','lori','alyssa','rosa',
  // Cross-cultural common names
  'mohammed','ahmed','ali','wei','chen','raj','priya','carlos','miguel','antonio','pablo',
  'marco','luca','hans','lars','sven','ivan','dmitri','yuki','hiroshi','kenji',
  'alejandro','ricardo','diego','luis','jorge','sofia','elena','lucia','ana','carmen',
  'fatima','aisha','omar','hassan','ibrahim',
  // — EXPANDED 2026-04-16 per audit —
  // South Asian (Indian, Pakistani, Bangladeshi, Sri Lankan) — common for immigration
  'arjun','aarav','arnav','akash','aakash','ansh','aryan','advik','atharv','ayaan',
  'rohan','rahul','rohit','rajesh','ramesh','rakesh','ravi','ritesh','rishabh','rishi',
  'vikram','vijay','vishal','vivek','varun','vinay','vinod','vinit','vishnu',
  'amit','anil','anand','ashok','ashish','abhay','abhinav','abhishek','aditya','akhil',
  'sanjay','sandeep','satish','sumit','sunil','suresh','sachin','saurabh','siddharth',
  'deepak','dinesh','dev','dhruv','dheeraj','nikhil','niraj','naveen','nitin','nilesh',
  'prakash','prashant','pawan','pramod','pranav','puneet','piyush','parth',
  'gaurav','gagan','gopal','girish','gautam','hemant','harsh','harshit','himanshu','kartik',
  'manoj','mahesh','manish','mohit','mukesh','mayank','krishna','karthik','keshav','kunal',
  'yash','yogesh','tarun','tushar','vibhor','arun','aman','amar','aryan',
  // Indian female
  'priya','pooja','priti','preeti','preeti','pallavi','parul','payal','prachi','pragati',
  'neha','nisha','nidhi','neeta','nalini','namrata','natasha','nimisha','nivedita',
  'anjali','anita','anu','anusha','anushka','aparna','apoorva','asha','ashwini','anamika',
  'divya','deepika','diksha','deepti','darshana','damini','devika','disha','dipti',
  'meera','madhavi','madhuri','malini','manisha','meenakshi','megha','mitali','monika','mrinalini',
  'radha','rachna','ragini','rajni','ranjana','rashmi','rekha','renu','revati','richa','riya','ruchi',
  'sangeeta','sandhya','sanjana','sarika','sarita','sangita','sapna','saumya','savita','seema','shagun',
  'shailja','shalini','shanti','sharada','sheetal','shikha','shilpa','shivani','shobha','shraddha',
  'shreya','shruti','shuchi','simran','smita','sneha','sonali','sonia','sonika','subhadra',
  'sudha','sujata','sumedha','sumita','sunanda','sunita','surekha','swati','tanvi','tanya','tejasvini',
  'urmila','usha','uttara','vaishali','vandana','varsha','vasudha','veena','vidya','vijaya',
  // Bangladeshi / Pakistani common
  'abdul','abdullah','ahsan','akbar','asif','atif','bilal','farhan','fawad','hamza','haroon',
  'imran','iqbal','irfan','jawad','kamran','muhammad','nabeel','nasir','rashid','saad','saif',
  'salman','shahid','shahzad','sohail','tariq','umar','usman','waqas','waseem','zafar','zain','zubair',
  // Korean
  'minjun','minsu','minho','junho','junwoo','jungho','jiho','jihun','jihoon','jinho','jisoo','jisung',
  'seojun','seungho','seongjun','seonghoon','sunghyun','sungmin','hyunwoo','hyunsoo','hyunjin',
  'dongju','donghyun','donghee','jaewon','jaehun','jaemin','kihoon','minhyuk','sangwoo','sanghoon',
  'taeyang','taehyun','taehyung','woohyun','yeonjun','yongsuk','youngjae',
  'jiyoung','jimin','jihye','jinyoung','jiyeon','jiwoo','hayoung','heeseo','hyejin','hyewon',
  'mijin','mirae','minji','minyoung','nari','seohyun','seoyeon','sohee','subin','suji','sumin','yoona',
  // Vietnamese (first names, usually last in full name but first in Western order)
  'long','hoang','thanh','thang','tung','tuan','trung','vinh','viet','hung','hieu','huy','khoi','khang',
  'lan','linh','mai','minh','ngoc','nga','nhung','phuong','quang','quynh','tam','thao','thao','thanh',
  'thuy','trang','tuyet','van','xuan','yen',
  // Filipino (many are Spanish-rooted)
  'angelo','angelito','arnold','benjamin','beverly','bong','carlo','carmelita','cesar','cristina',
  'daniela','dante','dennis','dexter','diosdado','dodong','edgar','edmundo','efren','elena',
  'erlinda','eva','evelyn','fernando','gerald','gina','gloria','grace','hilario','imelda','jose',
  'josephine','julius','lina','lita','lourdes','manuel','maribel','maricris','marilyn','marisol',
  'marlon','mateo','melinda','miguel','millie','nestor','norberto','ofelia','pedro','rafael','reggie',
  'regina','renato','rey','reynaldo','ricky','rosa','rosalinda','rosario','rose','roy','teresita','victor',
  // Chinese (pinyin — common in immigration context)
  'wei','ming','ping','jing','ling','yan','xin','xing','yun','yong','yang','yi','bo','bin',
  'dong','feng','gang','guo','hai','han','hong','hua','jian','jie','jun','lei','li','lin','liu',
  'long','lu','mei','na','nan','qi','qing','ru','shan','shen','tao','ting','wang','wen','xiao',
  'xiu','xue','ya','yao','ye','yong','yue','yun','zheng','zhi','zhong','zhou','zhu',
  // Japanese
  'akira','daiki','haruto','hiroki','hiroshi','kaito','kazuki','kenji','kenta','kota','kouki','makoto',
  'masato','naoki','ren','riku','sho','sota','taiki','takashi','takuya','tatsuya','yuki','yuma','yusuke',
  'ayaka','aya','chiharu','eri','hana','haruka','hinata','kana','kasumi','mai','mana','mei','misaki',
  'miyu','nanami','natsuki','noa','rio','sakura','satomi','sayaka','yui','yuka','yuki','yuna',
  // African (Nigerian, Ghanaian, Ethiopian, Kenyan)
  'adaeze','adaobi','adamu','adeola','ade','afolabi','aisha','akin','akinola','akwasi','amadi',
  'amina','aminata','anamichi','anu','ayo','bola','bongani','chike','chidi','chidimma','chiamaka',
  'chinedu','chinelo','chioma','dayo','dele','dinesh','ebo','eboni','efe','ejike','ekene','ekaete',
  'emeka','esther','fadeke','folake','folashade','gbenga','ibrahim','ifeoma','ify','ikechi','ikechukwu',
  'iyke','jide','kayode','kemi','kenny','kofi','kunle','kwaku','kwame','kwesi','mustafa','nana',
  'ngozi','nkem','nneka','obi','odetola','ola','olabisi','oluchi','olu','olumide','olusegun',
  'onyeka','osakwe','rotimi','segun','seyi','shola','sola','somto','sulaiman','tega','timi','tobi',
  'tokunbo','tunde','uche','udo','ugo','uzo','wale','wanjiru','yemi','yomi','yusuf','zainab','zara',
  // Eastern European (Polish, Russian, Ukrainian, Czech, Hungarian, Romanian)
  'aleksandr','aleksander','andrei','andriy','andrzej','anton','artem','artur','bartek','bogdan',
  'borys','dariusz','denys','dmitri','dmitry','dominik','dusan','feliks','filip','grzegorz','gyorgy',
  'igor','ilya','jakub','janusz','jerzy','jozef','karol','kasper','kiril','konstantin','krzysztof',
  'lukasz','maciej','marek','maksim','marcin','mateusz','michal','mikhail','milan','milos','mykola',
  'nikolai','oleksandr','oleksiy','oleg','oskar','pavel','pavlo','pawel','petr','piotr','rafal',
  'roman','rostislav','sergei','sergey','slavek','stanislaw','stefan','tadeusz','tomas','tomasz',
  'tomasz','vadim','valeri','viktor','vladimir','vladislav','wojciech','yaroslav','yuri','yuriy',
  'zbigniew','zdenek','anastasia','agata','aleksandra','alina','anna','barbara','beata','bogumila',
  'dagmara','daria','dominika','dorota','edyta','elzbieta','eugenia','ewa','galyna','halina','hanna',
  'hanna','iwona','jadwiga','jana','jolanta','justyna','karina','karolina','kasia','katarzyna',
  'katya','krystyna','larysa','lena','lidia','ludmila','magda','magdalena','malgorzata','maria',
  'maryna','mila','milena','natalya','natasha','olga','oksana','paulina','renata','roksana','sabina',
  'svetlana','tatyana','urszula','wanda','wanda','wioletta','yevheniya','yulia','zofia','zuzanna',
  // Nordic (Swedish, Norwegian, Danish, Finnish, Icelandic)
  'aksel','aleksi','andreas','anton','arne','bjorn','carl','einar','emil','erik','even','filip',
  'finn','fredrik','gustav','hakan','hans','harald','henning','henrik','ingvar','isak','jakob',
  'jens','jesper','jonas','jorgen','karl','kasper','knut','lars','leif','lennart','lucas','magnus',
  'mads','mathias','mikael','mikkel','mons','morten','niels','niklas','nils','odin','olaf','olav',
  'oskar','ove','patrik','per','petter','rasmus','rolf','sigurd','sigmund','stefan','stig','sven',
  'tobias','tor','torbjorn','tore','ulf','viktor','vidar','william',
  'agnes','alva','anna','annika','astrid','birgit','brigitta','camilla','cecilia','elin','elsa',
  'emma','erika','eva','felicia','frida','freya','gunnhild','hanna','helga','hilde','ida','ingrid',
  'jenny','johanna','julia','karin','kari','karoline','kristin','lena','lina','lisa','liv','lotta',
  'malin','maren','maria','marit','matilda','nina','oda','olga','pia','ragnhild','sara','sigrid',
  'silje','siri','sofia','solveig','stina','susanna','tea','thea','tilde','tove','tuva','vera','wilma',
  // Dutch / Belgian / German
  'aart','andre','bart','bastiaan','benno','casper','cornelis','daan','dennis','dirk','edwin',
  'erik','erwin','ewout','fons','frank','frans','fred','geert','gerrit','hans','harm','henk',
  'herman','hugo','jaap','jan','jelle','jeroen','jochem','johan','joop','joris','joost','jurgen',
  'kees','kevin','klaas','koen','lars','leo','luc','mart','martin','marten','maarten','mees',
  'michiel','mike','nick','niek','olaf','otto','patrick','paul','peer','peter','pieter','quincy',
  'rene','rick','robin','rob','roel','ron','ronald','ruben','rudolf','ruud','sander','sebastiaan',
  'sem','sjoerd','stefan','sven','teun','thijs','thomas','tjeerd','tom','tommy','tygo','vincent',
  'walter','wouter','agnes','andrea','anneke','annemieke','astrid','barbara','beatrix','bianca',
  'brigitte','carolien','charlotte','daniella','diana','dieuwertje','eefje','eline','elisabeth',
  'elke','emma','ester','eva','fenna','femke','floor','freya','greetje','gerda','hanneke','helga',
  'henny','ilse','ine','inge','ingrid','jacqueline','jannie','jeanette','jessica','johanna','judith',
  'karin','karina','kim','kirsten','klaartje','laura','leonie','lidy','liesbeth','lieke','lotte',
  'maaike','marian','marijke','marja','marlies','marloes','marga','maxime','melanie','mieke',
  'miriam','monique','nadia','natalie','nienke','nina','noor','petra','renske','ria','riet',
  'rietje','rosa','sabine','sandra','sanne','saskia','silvia','sophie','stefanie','suzanne',
  'sylvia','tessa','trudie','ursula','vera','wilma','yvonne','zoe',
  // Arabic / Middle Eastern
  'abbas','abdelrahman','abdul','abdulrahman','abdullah','abdelaziz','adel','ahmad','akram','alaa',
  'amin','amjad','anwar','arif','ashraf','ayman','bashar','bilal','farid','farouk','ghassan',
  'habib','hamid','hamza','haytham','hisham','hussain','ilyas','imad','ismail','jafar','jamal',
  'kamal','karim','khaled','khaldoun','mahmoud','malik','mansour','masood','mazen','mehmet',
  'muhammad','mukhtar','munir','murad','mustafa','nabil','nader','naji','naseem','nasir','nazim',
  'nidal','nizar','osama','ossama','qasim','rafi','rami','rashad','rashed','rashid','razi','reza',
  'riyad','saad','said','salah','salim','salman','samer','samir','sharif','sobhi','suhail','sultan',
  'tahir','talal','tamer','tarek','tariq','tawfiq','usman','walid','wassim','yahya','yasser','zaid',
  'zakaria','ziad','zubair','aida','aisha','alia','amal','amani','amina','amira','arwa','asma',
  'ayesha','bushra','dalia','dana','dina','eman','fadia','faiza','farah','farida','fatima','fatma',
  'ghada','ghazal','habiba','hafsa','hala','hanan','hanin','heba','hend','hiba','huda','iman',
  'khadija','lama','lamia','latifa','layla','leila','lina','maha','mai','malak','mariam','maryam',
  'meryem','mona','mounia','muna','nabila','nada','nadia','naima','najla','nawal','noor','nora',
  'nour','rabab','rabia','raghad','rahma','raina','rana','rasha','reem','rima','rola','rouba','saba',
  'safaa','saida','salma','salwa','samar','samia','sara','sawsan','shadia','shaima','shereen','soha',
  'suad','sumaya','tala','tasneem','wafaa','wasila','yara','yasmin','yasmine','zahra','zainab',
  'zeinab','zeyneb','zubaida',
  // Persian / Iranian
  'ali','amir','arash','arman','ashkan','babak','behnam','behzad','cyrus','daryush','ehsan','farhad',
  'farshad','fazel','fereydoun','hamid','hassan','hooman','iman','javad','kamran','kasra','keyvan',
  'kian','mahdi','mehran','mohsen','nader','navid','payam','pedram','poyan','rahim','reza','sadegh',
  'saeed','saman','sami','sasan','shahin','shahram','shayan','siavash','soroush','vahid','yousef',
  'azadeh','azita','donya','elham','fariba','farnaz','forough','gelareh','golnaz','haleh','leila',
  'mahsa','maryam','mehrnaz','mitra','nahid','naz','neda','negar','niloufar','parinaz','pariya',
  'parvaneh','roxana','sahar','saloumeh','shabnam','shadi','sharareh','sheida','shiva','sima',
  'soheila','solmaz','tara','yasaman',
  // Turkish
  'ahmet','ali','arda','bekir','berk','bora','burak','can','cem','cenk','deniz','ege','emir',
  'emre','ender','enes','enver','erdem','erkan','ertan','fatih','fuat','furkan','gokhan','halim',
  'halit','halil','haluk','hasan','hayri','ibrahim','ilhan','ismail','kadir','kaan','kemal','koray',
  'mehmet','mert','mesut','mithat','murat','mustafa','nejat','omer','onder','onur','orhan','osman',
  'ozgur','recep','sabri','sadi','sami','selim','sener','serkan','serhat','suleyman','taner','tarik',
  'tayfun','tolga','ufuk','ugur','umut','utku','yakup','yavuz','yasin','yunus','yusuf','aysegul',
  'aysel','aysenur','aylin','ayse','beyza','canan','cansu','ceren','ceyda','derya','didem','dilek',
  'duygu','ebru','eda','elif','emel','emine','emine','esma','esra','ezgi','fatma','feride','filiz',
  'gulay','gulcan','gulnur','gulsah','gunay','gul','hatice','havva','hayal','hayriye','hulya','iclal',
  'ipek','irem','leyla','lale','meltem','merve','melek','meryem','nalan','neslihan','nese','nil',
  'nur','nurcan','nurdan','nursel','nurten','ozge','pelin','pinar','rukiye','sakine','saliha','semiha',
  'sema','semra','sena','senem','serpil','sevda','sevgi','sevim','sibel','sueda','sule','sumeyye',
  'tuba','tugba','tulay','yagmur','yasemin','yesim','zekiye','zehra','zeynep','zeliha',
  // Greek
  'alexandros','andreas','anestis','antonios','apostolos','athanasios','christos','dimitris',
  'dimitrios','elias','evangelos','georgios','giorgos','harris','iannis','ioannis','kostas',
  'konstantinos','lefteris','manolis','markos','michalis','nikos','nikolaos','panagiotis','pavlos',
  'petros','savvas','spiros','stamatis','stavros','takis','theodoros','thomas','vangelis','vasilis',
  'yiannis','zisis','afroditi','alexia','angeliki','anna','anthi','areti','artemis','athina',
  'basia','chariklia','chrisoula','chrysa','dafni','despoina','dimitra','effie','eirini','elena',
  'eleni','elisavet','erato','evangelia','evdokia','eva','fani','foteini','georgia','giota',
  'ioanna','irini','kalliopi','kalomira','katerina','konstantina','loukia','magda','maria','marina',
  'marioliz','matina','melina','mirela','natasa','niki','olga','panagiota','pelagia','polina',
  'sofia','stamatina','stefania','theodora','thomai','vasiliki','virginia','xenia','yianna','zoi',
]);

// Words/phrases that are NOT person names — massively expanded
const FALSE_NAME_WORDS = new Set([
  // Navigation/UI elements
  'our team', 'the team', 'about us', 'contact us', 'get started',
  'learn more', 'read more', 'view more', 'see more', 'meet our',
  'our story', 'our mission', 'free consultation', 'schedule now',
  'book now', 'get in touch', 'call now', 'email us', 'follow us',
  'privacy policy', 'terms of service', 'all rights reserved',
  'powered by', 'designed by', 'built by', 'copyright',
  'quick links', 'main menu', 'site map', 'home page',
  'sign up', 'log in', 'sign in', 'get quote',
  'view all', 'load more', 'show more', 'see all',
  'next page', 'previous page', 'back home',
  // Marketing phrases
  'award winning', 'award-winning', 'top rated', 'best rated',
  'trusted advisors', 'trusted advisor', 'trusted partners',
  'quality care', 'quality service', 'expert care', 'expert service',
  'patient care', 'dental care', 'health care', 'home care',
  'family care', 'primary care', 'urgent care', 'elder care',
  'pain relief', 'pain management', 'pain free',
  'premier service', 'premium service', 'full service',
  'award winners', 'industry leaders', 'market leaders',
  'patients first', 'people first', 'clients first',
  'local experts', 'your experts', 'the experts',
  'real results', 'proven results', 'fast results',
  'free estimate', 'free estimates', 'free quote',
  'new patients', 'new clients', 'new customers',
  'special offers', 'current specials', 'latest news',
  'featured services', 'popular services', 'core services',
  'why us', 'why choose', 'how it works', 'what we do',
  // Section headings
  'practice areas', 'service areas', 'our services',
  'our locations', 'our offices', 'our partners',
  'case results', 'testimonials', 'client reviews',
  'latest posts', 'recent posts', 'blog posts',
  'news updates', 'press releases', 'media coverage',
  'photo gallery', 'image gallery', 'video gallery',
  'career opportunities', 'job openings', 'open positions',
  'community involvement', 'social responsibility',
  'professional memberships', 'board certifications',
  'office hours', 'business hours', 'opening hours',
  'virtual tour', 'office tour', 'facility tour',
  'before after', 'patient stories', 'success stories',
]);

// Individual words that should never appear in a person's name
const FALSE_NAME_COMPONENTS = new Set([
  'links', 'menu', 'care', 'service', 'services', 'award', 'winning',
  'rated', 'trusted', 'quality', 'expert', 'premier', 'premium',
  'results', 'offers', 'special', 'featured', 'popular', 'latest',
  'news', 'blog', 'post', 'posts', 'page', 'site', 'home',
  'gallery', 'tour', 'hours', 'area', 'areas', 'location', 'locations',
  'office', 'offices', 'reviews', 'review', 'testimonial', 'testimonials',
  'careers', 'career', 'jobs', 'virtual', 'online', 'free',
  'patients', 'clients', 'customers', 'members', 'visitors',
  'treatment', 'treatments', 'procedure', 'procedures', 'surgery',
  'insurance', 'payment', 'financing', 'pricing', 'cost',
  'appointment', 'appointments', 'schedule', 'booking',
  'emergency', 'urgent', 'immediate', 'same-day',
  'comprehensive', 'advanced', 'professional', 'certified',
  'experienced', 'dedicated', 'compassionate', 'innovative',
  'relief', 'management', 'prevention', 'recovery', 'wellness',
  'dental', 'medical', 'legal', 'financial',
  'orthodontics', 'pediatric', 'cosmetic', 'general',
  'first', 'best', 'top', 'leading', 'premier',
  'why', 'how', 'what', 'when', 'where', 'who',
  // Business name words that are NOT person names
  'clinic', 'center', 'centre', 'group', 'associates', 'partners',
  'firm', 'company', 'studio', 'agency', 'solutions', 'enterprises',
  'arts', 'works', 'labs', 'tech', 'systems', 'network',
  'north', 'south', 'east', 'west', 'central', 'metro',
  'city', 'county', 'state', 'national', 'international', 'global',
  'family', 'community', 'regional', 'downtown', 'midtown', 'uptown',
  'plaza', 'square', 'tower', 'building', 'suite', 'floor',
]);

// Title/role keywords that indicate a person's professional role
const TITLE_KEYWORDS = [
  'partner', 'associate', 'director', 'manager', 'founder',
  'owner', 'ceo', 'cto', 'cfo', 'coo', 'president', 'vp',
  'vice president', 'principal', 'counsel', 'attorney',
  'dentist', 'doctor', 'physician', 'surgeon', 'therapist',
  'broker', 'agent', 'consultant', 'advisor', 'planner',
  'engineer', 'technician', 'specialist', 'coordinator',
  'dds', 'dmd', 'md', 'do', 'esq', 'phd', 'rn', 'pa-c',
  'hygienist', 'nurse', 'paralegal', 'secretary',
  'accountant', 'analyst', 'architect', 'designer',
];

// Name validation regex — 2-4 word proper-case names
const NAME_REGEX = /^[A-Z][a-z]+(?:\s+[A-Z]\.?)?(?:\s+[A-Z][a-z]+){1,3}$/;

class PersonExtractor {
  constructor(options = {}) {
    this._browser = null;
    this._proxy = options.proxy;
  }

  async init() {
    let pup;
    try {
      const puppeteerExtra = require('puppeteer-extra');
      const StealthPlugin = require('puppeteer-extra-plugin-stealth');
      if (!puppeteerExtra._stealthRegistered) {
        puppeteerExtra.use(StealthPlugin());
        puppeteerExtra._stealthRegistered = true;
      }
      pup = puppeteerExtra;
    } catch {
      pup = puppeteer;
    }

    const launchOpts = {
      headless: 'new',
      protocolTimeout: 60000, // 60s timeout for CDP protocol commands
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-gpu',
        '--disable-blink-features=AutomationControlled',
        '--window-size=1280,900',
      ],
    };

    if (process.env.PUPPETEER_EXECUTABLE_PATH) {
      launchOpts.executablePath = process.env.PUPPETEER_EXECUTABLE_PATH;
    }

    this._browser = await pup.launch(launchOpts);
    log.info('[PersonExtractor] Browser launched');
  }

  async close() {
    if (this._browser) {
      await this._browser.close().catch(() => {});
      this._browser = null;
    }
  }

  /**
   * Extract people from a single website.
   * @param {string} website - Base URL of the business
   * @returns {object[]} Array of { first_name, last_name, title, email, phone, linkedin_url }
   */
  async extractPeople(website) {
    if (!this._browser) throw new Error('Browser not initialized — call init() first');
    if (!website) return [];

    // Normalize URL
    let baseUrl = website.trim();
    if (!/^https?:\/\//i.test(baseUrl)) baseUrl = 'https://' + baseUrl;
    baseUrl = baseUrl.replace(/\/+$/, '');

    const allPeople = new Map(); // Dedup by name

    const page = await this._browser.newPage();

    try {
      // Anti-detection patches
      await page.evaluateOnNewDocument(() => {
        Object.defineProperty(navigator, 'webdriver', { get: () => false });
        Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
      });
      await page.setUserAgent(
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
      );

      // Block heavy resources
      await page.setRequestInterception(true);
      page.on('request', (req) => {
        try {
          const type = req.resourceType();
          if (['image', 'media', 'font', 'stylesheet'].includes(type)) {
            req.abort();
          } else {
            req.continue();
          }
        } catch { /* request already handled */ }
      });

      await page.setViewport({ width: 1280, height: 900 });

      // 1. Load homepage — extract people AND discover team page links
      await this._extractFromUrl(page, baseUrl, allPeople);

      // 2. Find team/about links on the homepage (much faster than brute-forcing paths)
      const discoveredPaths = await page.evaluate((teamPaths) => {
        const found = new Set();
        const links = document.querySelectorAll('a[href]');
        for (const a of links) {
          const href = (a.getAttribute('href') || '').toLowerCase();
          const text = (a.textContent || '').toLowerCase().trim();
          // Match team/about links by href path or link text
          for (const p of teamPaths) {
            if (href.includes(p) || href.endsWith(p)) {
              found.add(a.getAttribute('href'));
              break;
            }
          }
          // Also match by link text
          if (/\b(team|staff|people|attorneys|lawyers|about|meet|our\s+team)\b/i.test(text)) {
            found.add(a.getAttribute('href'));
          }
        }
        return [...found].slice(0, 5); // Max 5 discovered links
      }, TEAM_PATHS);

      // 3. Visit discovered team pages first (these are known to exist)
      const visited = new Set();
      let miss404Count = 0;

      for (const href of discoveredPaths) {
        if (allPeople.size >= 50) break;

        let url;
        try {
          url = new URL(href, baseUrl).href;
        } catch { continue; }

        // Only visit same-domain pages
        if (!url.startsWith(baseUrl)) continue;
        if (url === baseUrl || url === baseUrl + '/') continue;
        if (visited.has(url)) continue;
        visited.add(url);

        try {
          const response = await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 8000 });
          if (response && response.status() === 200) {
            await this._extractFromUrl(page, url, allPeople, true);
          }
        } catch (err) {
          log.warn(`[PersonExtractor] Failed to visit ${url}: ${err.message}`);
        }
      }

      // 4. Only brute-force TEAM_PATHS if we found very few people so far
      if (allPeople.size < 3) {
        for (const path of TEAM_PATHS) {
          if (allPeople.size >= 50) break;
          if (miss404Count >= 3) break; // Site probably has no team pages

          const url = baseUrl + path;
          if (visited.has(url)) continue;
          visited.add(url);

          try {
            const response = await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 8000 });
            if (response && response.status() === 200) {
              miss404Count = 0; // Reset on success
              await this._extractFromUrl(page, url, allPeople, true);
            } else {
              miss404Count++;
            }
          } catch (err) {
            miss404Count++;
            log.warn(`[PersonExtractor] Failed to visit ${url}: ${err.message}`);
          }
        }
      }
    } catch (err) {
      log.warn(`[PersonExtractor] Error on ${baseUrl}: ${err.message}`);
    } finally {
      await page.close().catch(() => {});
    }

    return Array.from(allPeople.values());
  }

  /**
   * Extract people from a single page URL.
   */
  async _extractFromUrl(page, url, peopleMap, alreadyNavigated = false) {
    try {
      if (!alreadyNavigated) {
        await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 8000 });
      }

      await sleep(300); // Brief pause for JS rendering

      const pageData = await page.evaluate(() => {
        const data = { jsonLd: [], cards: [], headings: [], imgAlts: [], listItems: [] };

        // --- Strategy 1: JSON-LD ---
        const scripts = document.querySelectorAll('script[type="application/ld+json"]');
        for (const script of scripts) {
          try {
            const json = JSON.parse(script.textContent);
            const items = Array.isArray(json) ? json : [json];
            for (const item of items) {
              if (item['@type'] === 'Person' || item['@type'] === 'Physician' || item['@type'] === 'Dentist') {
                data.jsonLd.push({
                  name: item.name || '',
                  title: item.jobTitle || item.description || '',
                  email: item.email || '',
                  phone: item.telephone || '',
                  url: item.url || '',
                  image: item.image || '',
                });
              }
              // Check for Organization with employees
              if (item.employee) {
                const employees = Array.isArray(item.employee) ? item.employee : [item.employee];
                for (const emp of employees) {
                  if (typeof emp === 'object') {
                    data.jsonLd.push({
                      name: emp.name || '',
                      title: emp.jobTitle || '',
                      email: emp.email || '',
                      phone: emp.telephone || '',
                      url: emp.url || '',
                    });
                  }
                }
              }
              // Check for members
              if (item.member) {
                const members = Array.isArray(item.member) ? item.member : [item.member];
                for (const m of members) {
                  if (typeof m === 'object') {
                    data.jsonLd.push({
                      name: m.name || '',
                      title: m.jobTitle || '',
                      email: m.email || '',
                    });
                  }
                }
              }
            }
          } catch {}
        }

        // --- Strategy 2: Card patterns ---
        const cardSelectors = [
          '.team-member', '.staff-card', '.person', '.bio-card',
          '.team-card', '.member-card', '.profile-card', '.doctor-card',
          '.attorney-card', '.lawyer-card', '.provider-card',
          '[class*="team-member"]', '[class*="staff"]', '[class*="person-card"]',
          '[class*="bio-card"]', '[class*="team_member"]', '[class*="team-item"]',
          '.et_pb_team_member', '.elementor-team-member',
          '.wp-block-team-member',
        ];

        // Name element selectors (broader than just headings)
        const nameSelectors = 'h2, h3, h4, h5, .name, .title, [class*="name"]';
        const titleSelectors = '.position, .role, .designation, [class*="position"], [class*="role"], [class*="job"]';

        for (const sel of cardSelectors) {
          const cards = document.querySelectorAll(sel);
          for (const card of cards) {
            const nameEl = card.querySelector(nameSelectors);
            const titleEl = card.querySelector(titleSelectors);
            const emailEl = card.querySelector('a[href^="mailto:"]');
            const phoneEl = card.querySelector('a[href^="tel:"]');
            const linkedinEl = card.querySelector('a[href*="linkedin.com"]');

            if (nameEl) {
              data.cards.push({
                name: nameEl.textContent.trim(),
                title: titleEl ? titleEl.textContent.trim() : '',
                email: emailEl ? emailEl.href.replace('mailto:', '').split('?')[0] : '',
                phone: phoneEl ? phoneEl.href.replace('tel:', '') : '',
                linkedin: linkedinEl ? linkedinEl.href : '',
              });
            }
          }
        }

        // --- Strategy 2b: Repeating list patterns ---
        // Many firm sites use <ul class="results_list"><li> with nested divs for name/position/contact
        // Find <li> elements that contain both a name-like element and contact info
        if (data.cards.length === 0) {
          const listContainers = document.querySelectorAll('ul, ol');
          for (const list of listContainers) {
            const items = list.querySelectorAll(':scope > li');
            if (items.length < 3) continue; // Need at least 3 items to be a people list

            // Check if these list items have name + contact structure
            let nameCount = 0;
            for (let i = 0; i < Math.min(5, items.length); i++) {
              const li = items[i];
              const hasNameEl = li.querySelector('.title, .name, [class*="name"], [class*="title"]');
              const hasContact = li.querySelector('a[href^="tel:"], a[href^="mailto:"], .phone, .email, .contact');
              if (hasNameEl && hasContact) nameCount++;
            }

            // If most sampled items have name + contact, extract all
            if (nameCount >= 2) {
              for (const li of items) {
                const nameEl = li.querySelector('.title, .name, [class*="name"], [class*="title"]');
                const posEl = li.querySelector('.position, .role, .designation, [class*="position"], [class*="role"]');
                const emailEl = li.querySelector('a[href^="mailto:"]');
                const phoneEl = li.querySelector('a[href^="tel:"]');
                const linkedinEl = li.querySelector('a[href*="linkedin.com"]');

                if (nameEl) {
                  const nameText = nameEl.textContent.trim();
                  // Skip if name element also contains the position text
                  let posText = posEl ? posEl.textContent.trim() : '';

                  data.listItems.push({
                    name: nameText,
                    title: posText,
                    email: emailEl ? emailEl.href.replace('mailto:', '').split('?')[0] : '',
                    phone: phoneEl ? phoneEl.href.replace('tel:', '') : '',
                    linkedin: linkedinEl ? linkedinEl.href : '',
                  });
                }
              }
              // Continue to process other matching lists (sites often have one list per letter group)
            }
          }
        }

        // --- Strategy 3: Heading heuristic ---
        // Only extract from headings that appear to be in team/people sections
        const headings = document.querySelectorAll('h2, h3, h4');
        for (const h of headings) {
          const text = h.textContent.trim();
          if (text.length < 4 || text.length > 50) continue;
          // Skip common section heading patterns
          if (/^(our |the |meet |about |contact |service|practice|why |how |what |featured )/i.test(text)) continue;

          // Look for title/role in adjacent sibling
          let title = '';
          const nextEl = h.nextElementSibling;
          if (nextEl) {
            const nextText = nextEl.textContent.trim();
            if (nextText.length < 80) title = nextText;
          }

          // Look for email/phone/linkedin in nearby elements
          let email = '';
          let phone = '';
          let linkedin = '';
          const parent = h.parentElement;
          if (parent) {
            const emailLink = parent.querySelector('a[href^="mailto:"]');
            if (emailLink) email = emailLink.href.replace('mailto:', '').split('?')[0];
            const phoneLink = parent.querySelector('a[href^="tel:"]');
            if (phoneLink) phone = phoneLink.href.replace('tel:', '');
            const liLink = parent.querySelector('a[href*="linkedin.com"]');
            if (liLink) linkedin = liLink.href;
          }

          data.headings.push({ name: text, title, email, phone, linkedin });
        }

        // --- Strategy 4: Image alt text ---
        const imgs = document.querySelectorAll('img[alt]');
        for (const img of imgs) {
          const alt = img.getAttribute('alt') || '';
          if (alt.length < 4 || alt.length > 50) continue;
          if (/logo|icon|banner|header|background|placeholder|stock/i.test(alt)) continue;

          // Check if parent has card-like structure
          const parent = img.closest('div, article, li, figure');
          let title = '';
          if (parent) {
            const titleEl = parent.querySelector('.position, .role, .title, [class*="position"], [class*="role"]');
            if (titleEl) title = titleEl.textContent.trim();
          }

          data.imgAlts.push({ name: alt, title });
        }

        return data;
      });

      // Process extracted data into people
      // Priority: JSON-LD > Cards > Headings > Image Alts

      // JSON-LD people — highest trust, minimal validation
      for (const person of pageData.jsonLd) {
        this._addPerson(peopleMap, person.name, person.title, person.email, person.phone, person.url, 'jsonld');
      }

      // Card people — high trust (structured HTML), validate names
      for (const card of pageData.cards) {
        this._addPerson(peopleMap, card.name, card.title, card.email, card.phone, card.linkedin, 'card');
      }

      // List item people — medium-high trust (structured repeating pattern), validate names
      for (const item of pageData.listItems) {
        this._addPerson(peopleMap, item.name, item.title, item.email, item.phone, item.linkedin, 'card');
      }

      // Heading people — medium trust, STRICT name validation required
      for (const h of pageData.headings) {
        if (this._isLikelyHumanName(h.name)) {
          this._addPerson(peopleMap, h.name, h.title, h.email, h.phone, h.linkedin, 'heading');
        }
      }

      // Image alt people — low trust, STRICT name validation + must have nearby title
      for (const img of pageData.imgAlts) {
        if (this._isLikelyHumanName(img.name) && img.title && this._looksLikeTitle(img.title)) {
          this._addPerson(peopleMap, img.name, img.title, '', '', '', 'imgalt');
        }
      }
    } catch (err) {
      log.warn(`[PersonExtractor] Page extraction error on ${url}: ${err.message}`);
    }
  }

  /**
   * Strict check: is this text likely a real human name?
   * Uses common first names dictionary + structural validation.
   *
   * This is the key validation that prevents "Quick Links", "Health Care",
   * "Award-Winning Care", etc. from being treated as person names.
   */
  _isLikelyHumanName(text) {
    if (!text) return false;

    // Clean suffixes/titles
    const cleaned = text.trim()
      .replace(/,?\s*(jr\.?|sr\.?|iii?|iv|esq\.?|md|dds|dmd|phd|do|pa-c|rn|j\.?d\.?)$/gi, '')
      .trim();

    if (cleaned.length < 4 || cleaned.length > 40) return false;

    // Check against known false name phrases
    if (FALSE_NAME_WORDS.has(cleaned.toLowerCase())) return false;

    // Must not contain digits
    if (/\d/.test(cleaned)) return false;

    // Must not contain special characters (except hyphens, apostrophes, periods for initials)
    if (/[!@#$%^&*()+=\[\]{};:"|<>?/\\~`]/.test(cleaned)) return false;

    // Split into parts
    const parts = cleaned.split(/\s+/);
    if (parts.length < 2 || parts.length > 4) return false;

    // Check individual words against false name components
    for (const part of parts) {
      if (FALSE_NAME_COMPONENTS.has(part.toLowerCase())) return false;
    }

    // All parts must start with uppercase
    if (!parts.every(p => /^[A-Z]/.test(p))) return false;

    // KEY CHECK: At least the first word must match a common first name
    // This is what prevents "Quick Links", "Health Care", etc.
    const firstWord = parts[0].toLowerCase()
      .replace(/^dr$/i, '') // "Dr" prefix handled separately
      .replace(/\.$/, '');  // Remove trailing period

    // Allow "Dr" prefix — check second word
    if (/^dr\.?$/i.test(parts[0]) && parts.length >= 3) {
      return COMMON_FIRST_NAMES.has(parts[1].toLowerCase());
    }

    return COMMON_FIRST_NAMES.has(firstWord);
  }

  /**
   * Basic structural name check for JSON-LD and card-extracted names.
   * Less strict than _isLikelyHumanName — used when source is trusted.
   */
  _looksLikeName(text) {
    if (!text) return false;
    const cleaned = text.trim()
      .replace(/,?\s*(jr\.?|sr\.?|iii?|iv|esq\.?|md|dds|dmd|phd|do|pa-c|rn|j\.?d\.?)$/gi, '')
      .trim();

    if (cleaned.length < 4 || cleaned.length > 40) return false;
    if (FALSE_NAME_WORDS.has(cleaned.toLowerCase())) return false;
    if (/\d/.test(cleaned)) return false;

    const parts = cleaned.split(/\s+/);
    if (parts.length < 2 || parts.length > 4) return false;

    // Check individual words against false name components
    for (const part of parts) {
      if (FALSE_NAME_COMPONENTS.has(part.toLowerCase())) return false;
    }

    // All parts must start with uppercase
    return parts.every(p => /^[A-Z]/.test(p));
  }

  /**
   * Check if text looks like a professional title.
   */
  _looksLikeTitle(text) {
    if (!text || text.length > 100) return false;
    const lower = text.toLowerCase();
    return TITLE_KEYWORDS.some(kw => lower.includes(kw));
  }

  /**
   * Add a person to the map, deduplicating by normalized name.
   * @param {string} source - Extraction source: 'jsonld', 'card', 'heading', 'imgalt'
   */
  _addPerson(peopleMap, nameRaw, title, email, phone, linkedinUrl, source) {
    if (!nameRaw) return;

    // Clean name
    let name = nameRaw.trim()
      .replace(/,?\s*(jr\.?|sr\.?|iii?|iv|esq\.?|md|dds|dmd|phd|do|pa-c|rn|j\.?d\.?)$/gi, '')
      .trim();

    // For trusted sources (JSON-LD, cards), use basic validation
    // For untrusted sources (headings, img alts), use strict validation
    if (source === 'jsonld') {
      // JSON-LD is structured data — but many sites incorrectly mark businesses as Person
      if (name.length < 3 || !/\s/.test(name)) return;
      // Reject names containing business keywords (catches "Semidey Dental", "Smith Law Firm", etc.)
      const nameParts = name.split(/\s+/);
      if (nameParts.some(p => FALSE_NAME_COMPONENTS.has(p.toLowerCase()))) return;
    } else if (source === 'card') {
      if (!this._looksLikeName(name)) return;
    } else {
      // heading, imgalt — strict validation
      if (!this._isLikelyHumanName(name)) return;
    }

    // Handle ALL CAPS names (e.g., "JORGE HERNANDEZ" → "Jorge Hernandez")
    if (name === name.toUpperCase() && name.length > 3) {
      name = name.replace(/\b\w+/g, w => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase());
    }

    // Handle "Dr" prefix
    if (/^dr\.?\s+/i.test(name)) {
      name = name.replace(/^dr\.?\s+/i, '');
    }

    const parts = name.split(/\s+/);
    if (parts.length < 2) return;

    const firstName = parts[0];
    const lastName = parts[parts.length - 1];

    const key = `${firstName.toLowerCase()}_${lastName.toLowerCase()}`;

    // Only use title if it looks like a real professional title
    const cleanTitle = (title && this._looksLikeTitle(title)) ? title.trim() : '';

    // Clean email — must have user@domain.tld format
    const cleanEmail = (email || '').toLowerCase().trim();
    const validEmail = cleanEmail && /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(cleanEmail) && !cleanEmail.includes('example.com')
      ? cleanEmail : '';

    // Clean phone
    const cleanPhone = (phone || '').replace(/[^0-9+\-() .]/g, '').trim();

    // Clean LinkedIn
    let cleanLinkedIn = '';
    if (linkedinUrl && /linkedin\.com/i.test(linkedinUrl)) {
      cleanLinkedIn = linkedinUrl;
    }

    if (peopleMap.has(key)) {
      // Merge: fill missing fields
      const existing = peopleMap.get(key);
      if (!existing.title && cleanTitle) existing.title = cleanTitle;
      if (!existing.email && validEmail) existing.email = validEmail;
      if (!existing.phone && cleanPhone) existing.phone = cleanPhone;
      if (!existing.linkedin_url && cleanLinkedIn) existing.linkedin_url = cleanLinkedIn;
    } else {
      peopleMap.set(key, {
        first_name: firstName,
        last_name: lastName,
        title: cleanTitle,
        email: validEmail,
        phone: cleanPhone,
        linkedin_url: cleanLinkedIn,
      });
    }
  }

  /**
   * Extract people from multiple businesses.
   * @param {object[]} businesses - Array of { firm_name, website, city, state, phone }
   * @param {function} onProgress - Callback(current, total, businessName)
   * @param {function} isCancelled - Returns true if cancelled
   * @returns {{ peopleFound: number, websitesVisited: number, results: object[] }}
   */
  async batchExtract(businesses, onProgress, isCancelled) {
    const rateLimiter = new RateLimiter({ minDelay: 1000, maxDelay: 2000 });
    const results = [];
    let websitesVisited = 0;
    let totalPeople = 0;

    const withWebsite = businesses.filter(b => b.website);

    for (let i = 0; i < withWebsite.length; i++) {
      if (isCancelled && isCancelled()) break;

      const biz = withWebsite[i];

      if (onProgress) onProgress(i + 1, withWebsite.length, biz.firm_name || biz.website);

      await rateLimiter.wait();

      try {
        const people = await this.extractPeople(biz.website);
        websitesVisited++;

        for (const person of people) {
          // Inherit business data
          results.push({
            ...person,
            firm_name: biz.firm_name || '',
            city: biz.city || '',
            state: biz.state || '',
            website: biz.website || '',
            firm_phone: biz.phone || '',
          });
          totalPeople++;
        }

        if (people.length > 0) {
          log.info(`[PersonExtractor] Found ${people.length} people at ${biz.firm_name || biz.website}`);
        }
      } catch (err) {
        log.warn(`[PersonExtractor] Failed for ${biz.website}: ${err.message}`);
      }
    }

    return { peopleFound: totalPeople, websitesVisited, results };
  }
}

module.exports = PersonExtractor;
