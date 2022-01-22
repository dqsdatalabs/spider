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
import dateparser

class MySpider(Spider):
    name = 'austinhomes_london'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    thousand_separator = ','
    scale_separator = '.'
    start_urls = ["https://austinhomes.london/forrent/"]

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        try:
            data = json.loads(response.body)
            seen = False
            for item in data:
                follow_url = response.urljoin(item["url"])
                yield Request(follow_url, callback=self.populate_item)
                seen = True
            
        except:
            seen = False
            for item in response.xpath("//a[contains(.,'View Full Details')]/@href").extract():
                follow_url = response.urljoin(item)
                yield Request(follow_url, callback=self.populate_item)
                seen = True
        
        if page == 2 or seen:
            formdata = {
                "action": "get_ajax_posts",
                "propType": "forrent",
                "propStatus": "available",
                "pageSize": "10",
                "page": str(page),
                "orderby": "post_date",
                "order": "DESC",
                "metakey": "",
            }

            yield FormRequest(
                url="https://austinhomes.london/wp-admin/admin-ajax.php",
                callback=self.parse,
                formdata=formdata,
                meta={"page":page+1}
            )

    
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
 
        title = "".join(response.xpath("//title/text()").getall()).replace("Austin Homes", "")
        if get_p_type_string(title):
            item_loader.add_value("property_type", get_p_type_string(title))
        else:
            return

        item_loader.add_value("external_source", "Austinhomes_Co_PySpider_united_kingdom")
         
        item_loader.add_xpath("title", "//div[contains(@class,'title')]/text()")
        item_loader.add_xpath("zipcode", "//div[contains(@class,'postcode')]/text()")
        item_loader.add_xpath("room_count", "substring-before(//div/span[contains(.,'Room')]//text(),'Room')")
        item_loader.add_xpath("bathroom_count", "substring-before(//div/span[contains(.,'Bath')]//text(),'Bath')")

        rent = response.xpath("//div[contains(@class,'property-price')]/text()").extract_first()
        if rent:
            if "pw" in rent.lower():
                rent = rent.split('£')[-1].split('pw')[0].strip().replace(',', '.').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent)) * 4))
                item_loader.add_value("currency", 'GBP')
            else:
                item_loader.add_value("rent_string", rent.replace(",","."))   
     
        address = response.xpath("//div[strong[.='Address']]/text()[normalize-space()]").extract_first()       
        if address:
            item_loader.add_value("address", address)   
        
        square = response.xpath("//div[strong[.='Area']]/text()[normalize-space()]").extract_first()       
        if square and square.strip() !="0.00":
            item_loader.add_value("square_meters", int(float(square.strip().replace(",",".")))) 
  
        desc = " ".join(response.xpath("//div[@class='main-property-content']/p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
   
        parking = response.xpath("//div[@class='main-property-content']/p//text()[contains(.,'Parking.') or contains(.,'parking.')]").extract_first()
        if parking:
            item_loader.add_value("parking", True)
        terrace = response.xpath("//div[@class='main-property-content']/p//text()[contains(.,'terrace') or contains(.,'Terrace')]").extract_first()
        if terrace:
            item_loader.add_value("terrace", True)
        balcony = response.xpath("//div[@class='main-property-content']/p//text()[contains(.,'Balcony') or contains(.,'balcony')]").extract_first()
        if balcony:
            item_loader.add_value("balcony", True)
        swimming_pool = response.xpath("//div[@class='main-property-content' or @class='main-property-catchphrase']/p//text()[contains(.,'Swimming Pool')]").extract_first()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)

        furnished = response.xpath("//div[@class='main-property-content' or @class='main-property-catchphrase']//text()[contains(.,'Furnished') or contains(.,'furnished')]").extract_first()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
     
        script_map = response.xpath("//script/text()[contains(.,'lat:') and contains(.,'lng:')]").get()
        if script_map:  
            item_loader.add_value("latitude", script_map.split('lat:')[1].split(',')[0].strip())
            item_loader.add_value("longitude", script_map.split('lng:')[1].split('}')[0].strip())
          
        images = [x for x in response.xpath("//div[@class='main-property-images']//img/@src[normalize-space()]").extract()]
        if images:
            item_loader.add_value("images", images)    

        floor_plan_images  = [x for x in response.xpath("//div[@class='main-property-floorplan']/img/@src[normalize-space()]").extract()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images )    

        item_loader.add_value("landlord_phone", "0207 193 6985")
        item_loader.add_value("landlord_email", "info@austinhomes.london")
        item_loader.add_value("landlord_name", "Austin Homes London")    

        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and "house" in p_type_string.lower():
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    else:
        return None