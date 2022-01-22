# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re
from word2number import w2n
class MySpider(Spider):
    name = 'kwuk_com'        
    execution_type='testing' 
    country='united_kingdom'
    locale='en'
    external_source="Kwuk_PySpider_united_kingdom_en"
    custom_settings = {
        # "PROXY_ON":"True",
        "RETRY_HTTP_CODES": [500, 503, 504, 400, 401, 403, 405, 407, 408, 416, 456, 502, 429, 307]   
    }
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.kwuk.com/search/page/1/?department=residential-lettings&address_keyword&radius=5&minimum_price&maximum_price&minimum_rent&maximum_rent&property_type=30&minimum_bedrooms&maximum_bedrooms&include_new_homes=yes",
                ],
                "property_type" : "apartment",
                "p_type" : "30"
            },
            {
                "url" : [
                    "https://www.kwuk.com/search/page/1/?department=residential-lettings&address_keyword&radius=5&minimum_price&maximum_price&minimum_rent&maximum_rent&property_type=17&minimum_bedrooms&maximum_bedrooms&include_new_homes=yes",
                ],
                "property_type" : "house",
                "p_type" : "17"
            },   
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "p_type":url.get('p_type')})


    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath("//a[contains(.,'More Details')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            seen = True

        if page == 2 or seen:
            p_type = response.meta.get("p_type")
            p_url = f"https://www.kwuk.com/search/page/{page}/?department=residential-lettings&address_keyword&radius=5&minimum_price&maximum_price&minimum_rent&maximum_rent&property_type={p_type}&minimum_bedrooms&maximum_bedrooms&include_new_homes=yes"
            yield Request(p_url, callback=self.parse, meta={'property_type': response.meta.get('property_type'), "p_url":p_url})

            
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        summary = response.xpath("//div[@class='strapline']/text()").get()
        if summary and "Under Offer" in summary:
            return
        item_loader.add_value("external_source", self.external_source)
        title = response.xpath("//div/h1[contains(@class,'property_title')]//text()").extract_first()     
        if title:   
            item_loader.add_value("title",title.strip()) 
            item_loader.add_value("address",title.strip()) 
            item_loader.add_value("city",title.split(", ")[-1].strip()) 
  
        rent = response.xpath("//h3[@class='price']//text()").extract_first()
        if rent:
            if "pw" in rent:
                rent_numbers = re.findall(r'\d+(?:\.\d+)?', rent.replace(",","."))
                if rent_numbers:
                    rent = int(rent_numbers[0].replace(".",""))*4
                    rent = "£"+str(rent)
            item_loader.add_value("rent_string", rent.replace(",","."))
    
        room = response.xpath("//div/div[@class='strapline']//text()[contains(.,'bed')]").extract_first()
        if room:
            room_count = room.split(" bed")[0].strip()
            if room_count!="0":
                item_loader.add_value("room_count", room_count.strip())
        bathroom_count = response.xpath("//div[@class='features']//li[contains(.,'bathroom')]//text()").extract_first()
        if bathroom_count:  
            try:
                bathroom_count = bathroom_count.split("bathroom")[0].strip()
                bathroom_number = w2n.word_to_num(bathroom_count)
                item_loader.add_value("bathroom_count", bathroom_number)
            except:
                pass
        square_meters = response.xpath("//div[@class='features']//li[contains(.,'sq')]//text()").extract_first()
        if square_meters:
            unit_pattern = re.findall(r"[+-]? *((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*(Sq. Ft.|sqft|sq.ft|sq ft|sq. ft.|Sq. ft.|sq. Ft.|sq|Sq)",square_meters.replace(",",""))
            if unit_pattern:
                square_title=unit_pattern[0][0]
                sqm = str(int(float(square_title) * 0.09290304))
                item_loader.add_value("square_meters", sqm) 
        else:
            square_meters = response.xpath("//text()[contains(.,'sq/ft')]").get()
            if square_meters:
                square_meters = square_meters.split('sq/ft')[0].strip().split(' ')[-1].strip().replace(',', '.')
                try:
                    item_loader.add_value("square_meters", str(int(float(square_meters) * 0.09290304)))
                except:
                    pass

        furnished = response.xpath("//div[@class='features']//li[contains(.,'Furnished') or contains(.,'furnished')]//text()").extract_first()
        if furnished:
            if "furnished or unfurnished" in furnished.lower():
                pass
            elif "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)

        parking = response.xpath("//div[@class='features']//li[contains(.,'parking') or contains(.,'Parking')]//text()").extract_first()
        if parking:
            item_loader.add_value("parking", True)

        terrace = response.xpath("//div[@class='features']//li[contains(.,'Terrace') or contains(.,'terrace')]//text()").extract_first()
        if terrace:
            item_loader.add_value("terrace", True)

        balcony = response.xpath("//div[@class='features']//li[contains(.,'Balcon') or contains(.,'balcon')]//text()").extract_first()
        if balcony:
            item_loader.add_value("balcony", True)
        elevator = response.xpath("//div[@class='features']//li[contains(.,'Lift ') or contains(.,'lift ')]//text()").extract_first()
        if elevator:
            item_loader.add_value("elevator", True)

        swimming_pool = response.xpath("//div[@class='features']//li[contains(.,' pool') or contains(.,' Pool')]//text()").extract_first()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        pets = response.xpath("//div[@class='features']//li[contains(.,'Pets') or contains(.,'pets')]//text()").extract_first()
        if pets:
            item_loader.add_value("pets_allowed", True)

        map_coordinate = response.xpath("//script/text()[contains(.,'maps.LatLng(')]").extract_first()
        if map_coordinate:
            map_coordinate = map_coordinate.split('maps.LatLng(')[1].split(');')[0]
            latitude = map_coordinate.split(',')[0].strip()
            longitude = map_coordinate.split(',')[1].strip()
            if latitude and longitude:
                item_loader.add_value("longitude", longitude)
                item_loader.add_value("latitude", latitude)
            
        description = " ".join(response.xpath("//h2[contains(.,'Description')]/following-sibling::*//text()").getall()).strip()
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        external_id = response.xpath("//li[contains(.,'Ref') or contains(.,'ref')]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(':')[-1].strip())

        from datetime import datetime
        from datetime import date
        import dateparser
        available_date = response.xpath("//li[contains(.,'Available') or contains(.,'available')]/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.lower().split('available')[-1].strip(), date_formats=["%d/%m/%Y"], languages=['en'])
            today = datetime.combine(date.today(), datetime.min.time())
            if date_parsed:
                result = today > date_parsed
                if result == True:
                    date_parsed = date_parsed.replace(year = today.year + 1)
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
 
        images = [x for x in response.xpath("//div[@class='thumbnails']//ul/li//img/@src").extract()]
        if images:
            item_loader.add_value("images", images)      
        floor_images = [x for x in response.xpath("//div[@id='tab_floorplan']//img/@src").extract()]
        if floor_images:
            item_loader.add_value("floor_plan_images", floor_images)      
            
        landlord_name =response.xpath("//div[@class='agent-card']//text()[contains(.,'contact')]").extract_first()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.split("contact")[1].split(" on")[0].strip())
        else:
            item_loader.add_value("landlord_name", "Keller Williams UK")
        item_loader.add_value("landlord_phone", "020 7078 3939")
        item_loader.add_xpath("landlord_email", "//div[@class='agent-card']//a[contains(@href,'mail')]/text()")

        yield item_loader.load_item()
