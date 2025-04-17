export default {
	async fetch(request, env, ctx) {
	  const url = new URL(request.url);
	  const path = url.pathname;
  
	  try {
		// Route requests based on URL path
		if (path === '/api/sitemaps/acts') {
		  return await getSiteMapUrlsHandler();
		}
		if (path === '/api/sitemaps/cases') {
		  return await getCasesSiteMapUrlsHandler();
		}
		
		if (path === '/api/pages') {
		  return await getPageUrlsHandler(request);
		}
		
		if (path === '/api/scrape/acts') {
		return await scrapeActsPageHandler(request);
		}
  
		if (path === '/api/scrape/cases') {
		  return await scrapeCasesPageHandler(request);
		}
		// Default response for unmatched routes
		return new Response('Not found', { status: 404 });
	  } catch (error) {
		return new Response(`Server error: ${error.message}`, { 
		  status: 500,
		  headers: { 'Content-Type': 'text/plain' }
		});
	  }
	}
  };
  
  /**
   * Handler for GET /api/sitemaps
   */
  async function getSiteMapUrlsHandler() {
	try {
	  const urls = await getActsSiteMapUrls();
	  return new Response(JSON.stringify(urls), { 
		headers: { 'Content-Type': 'application/json' }
	  });
	} catch (error) {
	  return new Response(`Error fetching sitemaps: ${error.message}`, { status: 500 });
	}
  }

  async function getCasesSiteMapUrlsHandler() {
	try {
	  const urls = await getCasesSiteMapUrls();
	  return new Response(JSON.stringify(urls), { 
		headers: { 'Content-Type': 'application/json' }
	  });
	} catch (error) {
	  return new Response(`Error fetching sitemaps: ${error.message}`, { status: 500 });
	}
  }


  /**
   * Handler for GET /api/pages
   */
  async function getPageUrlsHandler(request) {
	try {
	  const url = new URL(request.url);
	  const sitemapUrl = url.searchParams.get('sitemap');
	  
	  if (!sitemapUrl) {
		return new Response('Missing sitemap URL parameter', { status: 400 });
	  }
	  
	  const urls = await getPageUrls(sitemapUrl);
	  return new Response(JSON.stringify(urls), {
		headers: { 'Content-Type': 'application/json' }
	  });
	} catch (error) {
	  return new Response(`Error fetching pages: ${error.message}`, { status: 500 });
	}
  }
  
  /**
   * Handler for GET /api/scrape
   */
  async function scrapeActsPageHandler(request) {
	try {
	  const url = new URL(request.url);
	  const pageUrl = url.searchParams.get('url');
	  
	  if (!pageUrl) {
		return new Response('Missing url parameter', { status: 400 });
	  }
	  
	  const data = await scrapeActsPage(pageUrl);
	  return new Response(JSON.stringify(data), {
		headers: { 'Content-Type': 'application/json' }
	  });
	} catch (error) {
	  return new Response(`Error scraping page: ${error.message}`, { status: 500 });
	}
  }
  

    async function scrapeCasesPageHandler(request) {
	try {
	  const url = new URL(request.url);
	  const pageUrl = url.searchParams.get('url');
	  
	  if (!pageUrl) {
		return new Response('Missing url parameter', { status: 400 });
	  }
	  
	  const data = await scrapeCasesPage(pageUrl);
	  return new Response(JSON.stringify(data), {
		headers: { 'Content-Type': 'application/json' }
	  });
	} catch (error) {
	  return new Response(`Error scraping page: ${error.message}`, { status: 500 });
	}
  }
  

  /**
   * Extracts URLs from XML sitemap using regex
   */
  function extractUrlsFromXml(xmlText) {
	const locRegex = /<loc>\s*(.*?)\s*<\/loc>/g;
	return [...xmlText.matchAll(locRegex)].map(match => match[1].trim());
  }
  
  /**
   * Fetch and parse sitemap URLs
   */
  async function getPageUrls(sitemapUrl) {
	const response = await fetch(sitemapUrl);
	if (!response.ok) throw new Error('Failed to fetch sitemap');
	return extractUrlsFromXml(await response.text());
  }
  
  /**
   * Get all sitemap URLs from index
   */
  async function getActsSiteMapUrls() {
	const response = await fetch('https://codes.findlaw.com/sitemapindex.xml');
	if (!response.ok) throw new Error('Failed to fetch sitemap index');
	return extractUrlsFromXml(await response.text());
  }
  


/**
   * Get all sitemap URLs from index
   */
  async function getCasesSiteMapUrls() {
	const response = await fetch('https://caselaw.findlaw.com/sitemapindex.xml');
	if (!response.ok) throw new Error('Failed to fetch sitemap index');
	return extractUrlsFromXml(await response.text());
  }
  
  
  function extractActDetails(text) {
	  /**
	   * Extracts the act name, chapter number, section ID, and section title from a given text.
	   *
	   * @param {string} text The input string containing the act details.
	   * @returns {object|null} An object containing the extracted components, or null if the format doesn't match.
	   */
  
	  // Improved regex to handle variations, including cases with letters in section ID
  
	  const sampleText = 'Minnesota Statutes Administration and Finance (Ch. 16A-16E) § 16E.19. Administration of state computer facilities'
	  const sampleText2 ="Code of Federal Regulations Title 43. Public Lands:  Interior § 43.3120.3–2 Filing of a nomination for competitive leasing"
  
	  const match = text.match(/(.+?)\s*(\(Ch\.?\s*[\w\-\.\s]+\))?\s*§\s*([\w\.\-]+)\.?\s*(.*)/i);
  
	  if (match) {
		  const actName = match[1] ? match[1].trim() : null;
		  const chapterNo = match[2] ? match[2].trim() : null;
		  const sectionTitle = match[4] ? match[4].trim() : null;
  
		  return {
			  act_name: actName,
			  chapter_no: chapterNo,
			  section_title: sectionTitle,
			  original: text,
		  };
	  }
	  return null;
  }
  
  /**
   * Scrape and structure page content
   */
  async function scrapeActsPage(url) {
	try {
	  const response = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' } });
	  if (!response.ok) throw new Error(`HTTP error ${response.status}`);
	  
	  const sectionData = {
		act_name: null,
		chapter_no: null,
		act_id: null,
		section_title: null,
		section_text: "",
		original: null
	  };
	  
	  // Extract act ID from URL
	  const actIdMatch = url.match(/\/([a-z0-9\-]+)\/$/i);
	  sectionData.act_id = actIdMatch ? actIdMatch[1] : "No Act ID found";
	  
	  // Use HTMLRewriter to parse content
	  const rewriter = new HTMLRewriter()
		.on('h1', {
		  text(text) {
			if (!sectionData.original) sectionData.original = '';
			sectionData.original += text.text;
		  }
		})
		.on('div.codes-content__text.codes-content__text--min-height', {
		  text(text) {
			sectionData.section_text += text.text;
		  }
		});
		
	  const transformed = rewriter.transform(response);
	  await transformed.text();
	  
	  // Clean up the extracted section text
	  sectionData.section_text = sectionData.section_text.replace(/\s+/g, ' ').trim();
	  
	  // Parse the title to extract act details
	  if (sectionData.original) {
		const actDetails = extractActDetails(sectionData.original);
		if (actDetails) {
		  sectionData.act_name = actDetails.act_name;
		  sectionData.chapter_no = actDetails.chapter_no;
		  sectionData.section_title = actDetails.section_title;
		}
	  }
	  
	  return {
		act_name: sectionData.act_name || 'Not found',
		chapter_no: sectionData.chapter_no || 'Not found',
		section_title: sectionData.section_title || 'Not found',
		section_text: sectionData.section_text || 'Not found',
		act_id: sectionData.act_id || 'Not found',
		original: sectionData.original
	  };
	} catch (error) {
	  return { error: error.message };
	}
  }
  
/**
 * Scrape and structure case law page content
 */
/**
 * Scrape and structure case law page content
 */
async function scrapeCasesPage(url) {
  try {
    const response = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' } });
    if (!response.ok) throw new Error(`HTTP error ${response.status}`);

    const caseData = {
      case_title: null,
      citation: null,
      docket_no: null,
      court: null,
      decided_date: null,
      content: "",
    };
    
    // Keep track of partial text for each paragraph
    let currentParagraph = "";
    let paragraphLabel = null;

    // Use HTMLRewriter to parse content
    const rewriter = new HTMLRewriter()
      .on('h1.caselaw-title', {
        text(text) {
          if (!caseData.case_title) {
            caseData.case_title = '';
          }
          caseData.case_title += text.text;
        }
      })
      .on('div.case-information p.case-information__paragraph', {
        element(element) {
          // Reset for each new paragraph
          currentParagraph = "";
          paragraphLabel = null;
        },
        text(text) {
          currentParagraph += text.text;
          
          // Check if this is the complete paragraph
          if (currentParagraph.includes('Citation:')) {
            caseData.citation = currentParagraph.replace('Citation:', '').trim();
          } else if (currentParagraph.includes('Court:')) {
            caseData.court = currentParagraph.replace('Court:', '').trim();
          } else if (currentParagraph.includes('Decided:')) {
            caseData.decided_date = currentParagraph.replace('Decided:', '').trim();
          } else if (currentParagraph.includes('Docket No:')) {
            caseData.docket_no = currentParagraph.replace('Docket No:', '').trim();
          }
        }
      })
      .on('div#caselaw-content', {
        text(text) {
          caseData.content += text.text;
        }
      });

    const transformed = rewriter.transform(response);
    await transformed.text();

    // Clean up the extracted data
    caseData.content = caseData.content.replace(/\s+/g, ' ').trim();
    if (caseData.case_title) {
      caseData.case_title = caseData.case_title.trim();
    }

	  return {
	  court_name: caseData.court || 'Not found',
      case_name: caseData.case_title || 'Not found',
     case_no: caseData.docket_no || 'Not found', 
	citation: caseData.citation || 'Not found',
      date: new Date(caseData.decided_date) || 'Not found',
      case_text: caseData.content || 'Not found',
    };
  } catch (error) {
    return { error: error.message };
  }
}