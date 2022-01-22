# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'greenwoodsresidential_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    thousand_separator = ','
    scale_separator = '.' 
    start_urls = ["https://www.greenwoodsresidential.co.uk/properties-to-let"]

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='property-tile']//h3/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)

        f_text = " ".join(response.xpath("//div[@id='propdescription']//text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            return
        item_loader.add_value("external_source", "Greenwoodsresidential_Co_PySpider_united_kingdom")
        item_loader.add_xpath("external_id", "substring-after(//div[@id='DetailsBox']//div[contains(.,'Ref ')]/text(),': ')")
     
        title = response.xpath("//div/h1/text()").extract_first()
        if title:
            item_loader.add_value("title",title.strip() )     
        address = ", ".join(response.xpath("//div[@id='DetailsBox']//address//text()").extract())
        if address:
            address = address.strip()
            item_loader.add_value("address", address)
            city_zip = address.split(",")[-1].strip()
            zipcode = city_zip.split(" ")[-2]+ " "+city_zip.split(" ")[-1]
            item_loader.add_value("city",city_zip.replace(zipcode,"").strip() )
            item_loader.add_value("zipcode",zipcode.strip() )
        rent = response.xpath("//div//small[@class='eapow-detail-price']//text()").extract_first()
        if rent:
            item_loader.add_value("rent_string",rent) 
        item_loader.add_xpath("room_count","//div[@id='PropertyRoomsIcons']/div/i[@class='flaticon-bed']/following-sibling::strong[1]/text()") 
        item_loader.add_xpath("bathroom_count","//div[@id='PropertyRoomsIcons']/div/i[@class='flaticon-bath']/following-sibling::strong[1]/text()") 
 
        furnished =response.xpath("//li[contains(.,'Furnished') or contains(.,'FURNISHED')]//text()").extract_first()    
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)
        parking =response.xpath("//li[contains(.,'parking') or contains(.,'PARKING')]//text()").extract_first()    
        if parking:
            item_loader.add_value("parking", True)
     
        images = [response.urljoin(x) for x in response.xpath("//div[@id='slider']/ul/li/img/@src").extract()]
        if images:
                item_loader.add_value("images", images)
        floor_plan_images = [response.urljoin(x) for x in response.xpath("//div[@id='eapowfloorplanplug']/div//a/img/@src").extract()]
        if floor_plan_images:
                item_loader.add_value("floor_plan_images", floor_plan_images)               
        script_map = response.xpath("//script[@type='text/javascript']/text()[contains(.,'lat:') and contains(.,'lon:') ]").get()
        if script_map:
            item_loader.add_value("latitude", script_map.split('lat: "')[1].split('",')[0].strip())
            item_loader.add_value("longitude", script_map.split('lon: "')[1].split('",')[0].strip())
        desc = " ".join(response.xpath("//div[contains(@class,'eapow-desc-wrapper')]/p//text() | //div[contains(@class,'eapow-desc-wrapper')]/div[2]//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        item_loader.add_value("landlord_name", "Greenwoods Residential")
        item_loader.add_value("landlord_phone", "020 8239 0535")
        item_loader.add_value("landlord_email", "lettings@greenwoodsresidential.co.uk")  
        
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "etage" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "woning" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "house"
    else:
        return None