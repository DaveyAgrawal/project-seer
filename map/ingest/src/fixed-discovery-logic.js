// Fixed discovery logic for EnergyNet listings page
// This logic properly extracts all listings from the actual page structure

const discoveredListings = await page.evaluate(() => {
  const listings = [];
  
  // Find all "View Listings" buttons - these are the key to each listing
  const viewListingButtons = Array.from(document.querySelectorAll('a[href*="govt_listing.pl?sg="]'));
  
  viewListingButtons.forEach((button, index) => {
    try {
      // Get the sale group ID from the href
      const href = button.getAttribute('href');
      const sgMatch = href.match(/sg=([^&]+)/);
      const saleGroup = sgMatch ? sgMatch[1] : `unknown-${index}`;
      
      // Find the parent container for this listing
      const container = button.closest('.row') || button.closest('div');
      if (!container) return;
      
      // Extract title from the lead paragraph
      const titleElement = container.querySelector('p.fs-5.fw-semibold.lead');
      const title = titleElement ? titleElement.textContent.trim() : 'Unknown Title';
      
      // Extract parcel info from the byline
      const bylineElement = container.querySelector('#byline_one, .text-secondary span');
      const bylineText = bylineElement ? bylineElement.textContent.trim() : '';
      
      // Parse region/state from title
      let region = 'Unknown';
      const titleLower = title.toLowerCase();
      if (titleLower.includes('wyoming')) region = 'Wyoming';
      else if (titleLower.includes('nevada')) region = 'Nevada';
      else if (titleLower.includes('new mexico')) region = 'New Mexico';
      else if (titleLower.includes('alaska')) region = 'Alaska';
      else if (titleLower.includes('utah')) region = 'Utah';
      else if (titleLower.includes('montana')) region = 'Montana';
      else if (titleLower.includes('colorado')) region = 'Colorado';
      else if (titleLower.includes('oklahoma')) region = 'Oklahoma';
      else if (titleLower.includes('las vegas')) region = 'Nevada';
      
      // Parse listing type
      let listingType = 'Oil & Gas Lease';
      if (titleLower.includes('geothermal')) listingType = 'Geothermal';
      else if (titleLower.includes('land sale')) listingType = 'Land Sale';
      
      // Extract agency from title
      let agency = 'Unknown Agency';
      if (titleLower.includes('blm')) {
        agency = `BLM ${region} State Office`;
      } else if (titleLower.includes('state oil & gas') || titleLower.includes('state land')) {
        agency = `${region} State Lands`;
      } else if (titleLower.includes('dnr')) {
        agency = `${region} DNR`;
      } else if (titleLower.includes('clo')) {
        agency = `${region} CLO`;
      } else if (titleLower.includes('city of las vegas')) {
        agency = 'City of Las Vegas';
      }
      
      // Extract date information
      const dateElement = container.querySelector('small.text-center');
      let saleStartDate = null;
      let saleEndDate = null;
      
      if (dateElement) {
        const dateText = dateElement.textContent;
        // This would need more sophisticated date parsing
        console.log(`Date text for ${saleGroup}: ${dateText}`);
      }
      
      listings.push({
        saleGroup,
        listingId: saleGroup,
        title,
        region,
        listingType,
        agency,
        url: `https://www.energynet.com/govt_listing.pl?sg=${saleGroup}`,
        status: 'active',
        byline: bylineText
      });
      
    } catch (error) {
      console.log(`Error parsing listing ${index}:`, error);
    }
  });
  
  return listings;
});

console.log(`✅ Discovered ${discoveredListings.length} active listings`);
discoveredListings.forEach(listing => {
  console.log(`   📋 ${listing.saleGroup}: ${listing.region} - ${listing.listingType}`);
});

return discoveredListings;