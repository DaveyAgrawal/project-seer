const { chromium } = require('playwright');
const fs = require('fs');

async function testEnergyNet() {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();
  
  console.log('Navigating to EnergyNet...');
  await page.goto('https://www.energynet.com/govt_listing.pl', {
    waitUntil: 'networkidle',
    timeout: 60000
  });
  
  // Wait for dynamic content
  await page.waitForTimeout(5000);
  
  // Find all links with /salegroup/ pattern (new URL format)
  const links = await page.evaluate(() => {
    const allLinks = Array.from(document.querySelectorAll('a[href*="/salegroup/"]'));
    return allLinks.map(a => {
      const href = a.getAttribute('href');
      const match = href.match(/\/salegroup\/(\d+)/);
      const container = a.closest('.rounded-lg') || a.closest('.border') || a.closest('div');
      let title = 'Unknown';
      if (container) {
        const titleEl = container.querySelector('h3') || container.querySelector('.font-semibold');
        if (titleEl) title = titleEl.textContent?.trim();
      }
      return { 
        saleGroup: match ? match[1] : 'unknown',
        href: href,
        title: title
      };
    });
  });
  
  console.log(`\nFound ${links.length} salegroup links:`);
  links.forEach(l => console.log(`  - ${l.saleGroup}: ${l.title}`));
  
  await browser.close();
}

testEnergyNet().catch(console.error);
