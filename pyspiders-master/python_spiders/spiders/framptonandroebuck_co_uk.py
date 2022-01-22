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
    name = 'framptonandroebuck_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    thousand_separator = ','
    scale_separator = '.'
    external_source = "Framptonandroebuck_Co_PySpider_united_kingdom"
    def start_requests(self):

        start_urls = [
            {
                "url" : "https://www.framptonandroebuck.co.uk/residential/",
                "p_type" : None
            },
            {
                "url" : "https://www.framptonandroebuck.co.uk/students/",
                "p_type" : "student_apartment"
            }
        ]

        for item in start_urls:
            yield Request(
                url = item.get("url"),
                callback=self.parse,
                meta={
                    "p_type" : item.get("p_type")
                }
            )

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[@class='contents']/@href").extract():
            yield Request(item, callback=self.populate_item, meta={"p_type":response.meta.get("p_type")})
        
      
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)

        p_type = response.meta.get("p_type")
        if p_type:
            item_loader.add_value("property_type", p_type)
        else:
            desc = " ".join(response.xpath("//div[@class='property-description bg-grey']//text()[.!='Property Description'][normalize-space()]").getall())
            if get_p_type_string(desc):
                item_loader.add_value("property_type", get_p_type_string(desc))
            else:
                return

        item_loader.add_value("external_source", self.external_source)
        title = response.xpath("//div[@class='inner-top']//div[@class='title']/text()").extract_first()
        if title:
            item_loader.add_value("title", title.strip())
            item_loader.add_value("address", title.strip())
            # item_loader.add_value("zipcode", title.split(",")[-1].strip())
            item_loader.add_value("city", title.split(",")[-1].strip())
        item_loader.add_xpath("deposit", "//div[div[.='Deposit:']]/div[2]/text()")
    
        room_count = response.xpath("//div[@class='inner-top']//div[i[@class='fas fa-bed']]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)

        bathroom_count=response.xpath("//div[@class='inner-top']//div[i[@class='fas fa-bath']]/text()").get()
        if bathroom_count:    
            item_loader.add_value("bathroom_count", bathroom_count)

        rent = response.xpath("//div[@class='inner-top']//div[@class='rent']/text()").extract_first()
        if rent:
            if "pw" in rent.lower():
                rent = rent.split('£')[-1].lower().split(' p')[0].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent)) * 4))
            else:
                item_loader.add_value("rent_string", rent)    
        item_loader.add_value("currency", 'GBP')
        
        desc = " ".join(response.xpath("//div[@class='property-description bg-grey']//text()[.!='Property Description'][normalize-space()]").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
        available_date = response.xpath("//div[@class='availability']//span[contains(.,'Available From ')]//text()").extract_first()
        if available_date:
            try:
                date_parsed = dateparser.parse(available_date.split("From")[1].strip(), languages=['en'])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
            except:
                pass
        floor = response.xpath("//div[@class='feature']/text()[contains(.,'Floor') and not(contains(.,'NEW'))]").extract_first()
        if floor:
            floor = floor.split("Floor")[0].strip()
            item_loader.add_value("floor", floor)

        furnished = response.xpath("//div[@class='feature']/text()[contains(.,'Furnished') or contains(.,'furnished') or contains(.,'Furniture')]").extract_first()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
        parking = response.xpath("//div[@class='feature']/text()[contains(.,'Parking') or contains(.,'parking') ]").extract_first()
        if parking:
            item_loader.add_value("parking", True)
      
        images = [x for x in response.xpath("//div[@class='image-slider-modal']//div[contains(@class,'image-cont')]/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)    
 
        item_loader.add_value("landlord_phone", "01913832123")
        item_loader.add_value("landlord_email", "info@framptonandroebuck.co.uk")
        item_loader.add_value("landlord_name", "Frampton and Roebuck")     
   
    
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and "house" in p_type_string.lower():
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None
