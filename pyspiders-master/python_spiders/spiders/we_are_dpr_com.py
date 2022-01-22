# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from word2number import w2n

class MySpider(Spider):
    name = 'we_are_dpr_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    thousand_separator = ','
    scale_separator = '.' 
    start_urls = ["https://we-are-dpr.com/search_results/?address_keyword=&radius=&minimum_bedrooms=&maximum_price=&maximum_rent=&department=residential-lettings&post_type=property"]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='actions']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://we-are-dpr.com/search_results/page/{page}/?address_keyword&radius&minimum_bedrooms&maximum_price&maximum_rent&department=residential-lettings&post_type=property"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        
        f_text = " ".join(response.xpath("//div[contains(@class,'features')]//text()").getall())
        prop_type = ""
        if get_p_type_string(f_text):
            prop_type = get_p_type_string(f_text)
        else:
            f_text = " ".join(response.xpath("//div[contains(@class,'description')]//text()").getall())
            if get_p_type_string(f_text):
                prop_type = get_p_type_string(f_text)

        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else:
            return

        if response.xpath("//div[contains(text(),'This property is not currently available')]").get(): return

        item_loader.add_value("external_source", "We_Are_Dpr_PySpider_united_kingdom")      
        title = response.xpath("//div/h1//text()").extract_first()
        if title:
            item_loader.add_value("title",title)   
        external_id = response.xpath("//link[@rel='shortlink']/@href").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split("?p=")[-1])
        address = response.xpath("//div/h1//text()").extract_first()
        if address:
            item_loader.add_value("address",address)   
            item_loader.add_value("city",address.split(",")[-2].strip())   
            item_loader.add_value("zipcode",address.split(",")[-1].strip())  
                       
        rent = response.xpath("//div[contains(@class,'property-price')]//text()").extract_first()
        if rent:
            if "pcm" in rent:
               rent = rent.split("pcm")[0] 
            item_loader.add_value("rent_string", rent)

        room_count = response.xpath("//div[@class='feature'][i[@class='fas fa-bed']]/text()").extract_first() 
        if room_count:   
            room_count = room_count.strip().split("Bed")[0]
            item_loader.add_value("room_count", room_count)
        bathroom = response.xpath("substring-before(//div[@class='features']/ul/li[contains(.,'bathrooms')]//text(),'bathrooms')").extract_first()
        if bathroom:            
            bathroom_count = w2n.word_to_num(bathroom.strip() )
            item_loader.add_value("bathroom_count",bathroom_count) 
  
        terrace = response.xpath("//div[@class='features']/ul/li[contains(.,'terrace')]//text()").extract_first()
        if terrace:
            item_loader.add_value("terrace", True)
        balcony = response.xpath("//div[@class='features']/ul/li[contains(.,'Balcony') or contains(.,'balcony')]//text()").extract_first()
        if balcony:
            item_loader.add_value("balcony", True)
        swimming_pool = response.xpath("//div[@class='features']/ul/li[.='Pool']//text()").extract_first()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        furnished = response.xpath("//div[@class='features']/ul/li[contains(.,'furnished') or contains(.,'Furnished')]//text()").extract_first()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)

        parking = response.xpath("//div[@class='features']/ul/li[contains(.,'Parking') or contains(.,'parking')]//text()").extract_first()
        if parking:
            item_loader.add_value("parking",True)                
        desc = " ".join(response.xpath("//div[contains(@class,'description')]/p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        images = [response.urljoin(x) for x in response.xpath("//div[@id='slider']/ul/li/a/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)
        script_map = response.xpath("//script[contains(.,'google.maps.LatLng(')]/text()").get()
        if script_map:
            latlng = script_map.split("google.maps.LatLng(")[1].split(");")[0]
            item_loader.add_value("latitude", latlng.split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split(",")[1].strip())
        
        if not item_loader.get_collected_values("bathroom_count"):
            bathroom_count = response.xpath("//li[contains(.,'Bathrooms')]/text()").get()
            if bathroom_count: item_loader.add_value("bathroom_count", "".join(filter(str.isnumeric, bathroom_count)))
 
        item_loader.add_value("landlord_name", "Docklands Prestige Residential")
        item_loader.add_value("landlord_phone", "020 7511 6311")
        item_loader.add_value("landlord_email", "info@we-are-dpr.com")       
        
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "woning" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"
    else:
        return None