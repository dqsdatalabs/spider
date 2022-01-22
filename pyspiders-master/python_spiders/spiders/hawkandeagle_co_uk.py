# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser



class MySpider(Spider):
    name = 'hawkandeagle_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'   
    def start_requests(self):
        start_urls = [
            {"url": "http://www.hawkandeagle.co.uk/let/property-to-let/"},            
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                        )

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='modules']/div[contains(@class,'results-gallery')]//div[@class='featured-address']/a/@href").extract():
            follow_url = response.urljoin(item)
            p_type = item.split("/")[-2]
            if "-share" in p_type or "room" in p_type:
                property_type = "room"
            elif "apartment" in p_type or "flat" in p_type:
                property_type = "apartment"
            elif "studio" in p_type:
                property_type = "studio"
            else:
                property_type = None
            
            if property_type:
                yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})

        pagination = response.xpath("//ul[@class='pagination']/li/a[contains(.,'»')]/@href").get()
        if pagination:
            yield Request(pagination, callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        title = " ".join(response.xpath("//h1/text()").extract())
        if title:
            item_loader.add_value("title", title.strip())
            item_loader.add_value("address", title.strip())
            item_loader.add_value("city", title.split(",")[-2].strip())
            item_loader.add_value("zipcode", title.split(",")[-1].strip())
       
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Hawkandeagle_Co_PySpider_united_kingdom")
 
        room_count = response.xpath("//div[contains(@class,'property-actions-area')]//span[contains(.,'BEDROOM')]//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split("BEDROOM")[0])
        elif "studio" in response.meta.get('property_type') or "room" in response.meta.get('property_type'):
            item_loader.add_value("room_count", "1")
        bathroom_count=response.xpath("//div[contains(@class,'property-actions-area')]//span[contains(.,'BATHROOM')]//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split("BATHROOM")[0])
        elif not bathroom_count:
            bathroom_count = response.xpath("//div[contains(@class,'main-property-features')]/div/div[contains(.,'Bathroom')]/text()").get()
            if bathroom_count:
                bathroom_count = bathroom_count.strip().split(" ")[0]
                if bathroom_count.isdigit():
                    item_loader.add_value("bathroom_count", bathroom_count) 

        available_date = response.xpath("//div[contains(@class,'main-property-features')]/div/div[contains(.,'Available')]/text()[not(contains(.,'Now'))]").get()
        if available_date:
            try:
                date_parsed = dateparser.parse(available_date.split("Available from")[1].strip(), languages=['en'])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
            except:
                pass
        floor = response.xpath("//div[contains(@class,'main-property-features')]/div/div[contains(.,'Floor')]/text()[not(contains(.,'Flooring'))]").get()
        if floor:
            item_loader.add_value("floor", floor.split("Floor")[0].strip()) 
        map_coordinate = response.xpath("//script/text()[contains(.,'data:') and contains(.,'lng')]").extract_first()
        if map_coordinate:
            latitude = map_coordinate.split("lat': '")[1].split("',")[0].strip()
            longitude = map_coordinate.split("'lng': '")[1].split("'}")[0].strip()
            if latitude and longitude:
                item_loader.add_value("longitude", longitude)
                item_loader.add_value("latitude", latitude)
            
        balcony = response.xpath("//div[contains(@class,'main-property-features')]/div/div[contains(.,'balcony') or contains(.,'Balcony')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True) 
        parking = response.xpath("//div[contains(@class,'main-property-features')]/div/div[contains(.,'parking') or contains(.,'Parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True) 

        furnished = response.xpath("//div[contains(@class,'main-property-features')]/div/div[contains(.,'Furnished') or contains(.,'furnished')]/text()").get()
        if furnished:
            if "Furnished or Unfurnished" in furnished:
                pass
            elif "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False) 
            else:
                item_loader.add_value("furnished", True) 

        rent =" ".join( response.xpath("//div[contains(@class,'property-details-header')]//h2/text()").extract())
        if rent:
            if "(" in rent and "()" not in rent:
                rent = rent.split("(")[1]
            elif "week" in rent:
                rent = rent.split('£')[1].split('week')[0].replace("per","").strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent)) * 4))
                item_loader.add_value("currency", 'GBP')

            item_loader.add_value("rent_string", rent)    

       
        desc = " ".join(response.xpath("//div[contains(@class,'property-details')]/p//text()").extract())
        if desc:
            item_loader.add_value("description",desc.strip())
     
        images = [response.urljoin(x) for x in response.xpath("//div[contains(@class,'imageviewerPartial')]/div/@data-image-src").extract()]
        if images:
            item_loader.add_value("images", images)   
    
        item_loader.add_value("landlord_phone", "020 7709 8410")
        item_loader.add_value("landlord_email", "info@hawkandeagle.co.uk")
        item_loader.add_value("landlord_name", "HAWK & EAGLE PROPERTY CONSULTANTS")
        yield item_loader.load_item()