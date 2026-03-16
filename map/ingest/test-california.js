const { chromium } = require('playwright');
const fs = require('fs');

async function testCalifornia() {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();
  
  // Test the California listing page
  const url = 'https://www.energynet.com/salegroup/6472';
  console.log(`Navigating to: ${url}`);
  
  await page.goto(url, {
    waitUntil: 'networkidle',
    timeout: 60000
  });
  
  await page.waitForTimeout(5000);
  
  const html = await page.content();
  fs.writeFileSync('california-debug.html', html);
  console.log(`Page HTML length: ${html.length}`);
  console.log('Saved to california-debug.html');
  
  // Look for GIS download links
  const gisLinks = await page.evaluate(() => {
    const links = Array.from(document.querySelectorAll('a'));
    return links
      .filter(a => {
        const text = a.textContent?.toLowerCase() || '';
        const href = a.href?.toLowerCase() || '';
        return text.includes('gis') || text.includes('download') || 
               href.includes('.zip') || href.includes('.kml') || href.includes('.shp');
      })
      .map(a => ({ text: a.textContent?.trim(), href: a.href }));
  });
  
  console.log(`\nFound ${gisLinks.length} potential GIS/download links:`);
  gisLinks.forEach(l => console.log(`  - ${l.text}: ${l.href}`));
  
  // Look for title
  const title = await page.evaluate(() => {
    const h1 = document.querySelector('h1');
    const h2 = document.querySelector('h2');
    return { h1: h1?.textContent?.trim(), h2: h2?.textContent?.trim() };
  });
  console.log(`\nTitle elements:`, title);
  
  await browser.close();
}

testCalifornia().catch(console.error);
