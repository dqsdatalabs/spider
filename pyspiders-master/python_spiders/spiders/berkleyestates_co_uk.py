# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader

class MySpider(Spider):
    name = 'berkleyestates_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'    
    thousand_separator = ','
    scale_separator = '.'  
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://berkleyestates.co.uk/search-results/?type=rent&q=&property-type=1095&min-bedrooms=&min-price=&max-price=&view=&per-page=&sort=&polygon=",
                    "https://berkleyestates.co.uk/search-results/?type=rent&q=&property-type=1081&min-bedrooms=&min-price=&max-price=&view=&per-page=&sort=&polygon=",

                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://berkleyestates.co.uk/search-results/?type=rent&q=&property-type=1121&min-bedrooms=&min-price=&max-price=&view=&per-page=&sort=&polygon=",
                    "https://berkleyestates.co.uk/search-results/?type=rent&q=&property-type=1117&min-bedrooms=&min-price=&max-price=&view=&per-page=&sort=&polygon=",
                    "https://berkleyestates.co.uk/search-results/?type=rent&q=&property-type=1087&min-bedrooms=&min-price=&max-price=&view=&per-page=&sort=&polygon=",
                    "https://berkleyestates.co.uk/search-results/?type=rent&q=&property-type=1085&min-bedrooms=&min-price=&max-price=&view=&per-page=&sort=&polygon=",
                    "https://berkleyestates.co.uk/search-results/?type=rent&q=&property-type=1116&min-bedrooms=&min-price=&max-price=&view=&per-page=&sort=&polygon=",
                    "https://berkleyestates.co.uk/search-results/?type=rent&q=&property-type=1093&min-bedrooms=&min-price=&max-price=&view=&per-page=&sort=&polygon=",
                    "https://berkleyestates.co.uk/search-results/?type=rent&q=&property-type=1092&min-bedrooms=&min-price=&max-price=&view=&per-page=&sort=&polygon=",
                    "https://berkleyestates.co.uk/search-results/?type=rent&q=&property-type=1094&min-bedrooms=&min-price=&max-price=&view=&per-page=&sort=&polygon="
                ],
                "property_type": "house"
            },
            {
                "url": [
                    "https://berkleyestates.co.uk/search-results/?type=rent&q=&property-type=1089&min-bedrooms=&min-price=&max-price=&view=&per-page=&sort=&polygon=",
                   ],
                "property_type": "studio"
            }
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='_expertweb-property']"):
            url = item.xpath(".//div[@class='_expertweb-property-view-button']/a/@href").get()
            let = item.xpath(".//div[@class='_expertweb-property-status']/text()[.='Let Agreed']").get()
            if let:
                continue            
            yield Request(url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
  
        next_page = response.xpath("//li[@class='page-item']/a[.='›']/@href").get()
        if next_page:
            yield Request(next_page, callback=self.parse, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("property/")[-1].split("-")[1])
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Berkleyestates_Co_PySpider_united_kingdom")
        item_loader.add_xpath("title", "//h1[@class='_expertweb-property-address']/text()")
        address = response.xpath("//h1[@class='_expertweb-property-address']/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(",")[-1])
       
        item_loader.add_xpath("rent_string", "//div[@class='_expertweb-property-price']/text()")
        desc = " ".join(response.xpath("//div[@class='_expertweb-property-description']/p//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
 
        item_loader.add_xpath("room_count", "substring-before(//div[@class='_expertweb-property-rooms']/span/text()[contains(.,'Bedroom')],'Bedroom')")
        item_loader.add_xpath("bathroom_count", "substring-before(//div[@class='_expertweb-property-rooms']/span/text()[contains(.,'Bathroom')],'Bathroom')")
        floor = response.xpath("//li[contains(.,'floor ')]//text()").get()
        if floor:
            item_loader.add_value("floor", floor.split("floor ")[0])
        balcony = response.xpath("//li[contains(.,'balcony' or contains(.,'Balcony'))]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        parking = response.xpath("//li[contains(.,'parking') or contains(.,'Parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        furnished = response.xpath("//li[contains(.,'furnished') or contains(.,'Furnished')]//text()").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)
        latitude_longitude = response.xpath("//script[contains(.,'var _coordinates =') and contains(.,'latitude')]/text()").get()
        if latitude_longitude:     
            item_loader.add_value("latitude", latitude_longitude.split("'latitude': '")[1].split("'")[0])
            item_loader.add_value("longitude", latitude_longitude.split("'longitude': '")[1].split("'")[0])
      
        images = [x for x in response.xpath("//div[@class='carousel-inner']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)        
        floor_plan_images = [x for x in response.xpath("//div[@class='_expertweb-property-media-button-container']//span[contains(.,'Floorplans')]//img/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)        
     
        item_loader.add_value("landlord_name", "Berkley Estate Agents")
        item_loader.add_value("landlord_phone", "0116 254 4755")
        item_loader.add_value("landlord_email", "leicester@berkleyestates.co.uk")
   
        yield item_loader.load_item()