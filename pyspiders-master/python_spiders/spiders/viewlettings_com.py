# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader

class MySpider(Spider):
    name = 'viewlettings_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'
    external_source="ViewLettings_PySpider_united_kingdom"
    custom_settings = {
        "HTTPCACHE_ENABLED": False,
    } 
    

    def start_requests(self):
        start_url = "https://www.viewlettings.com/properties-to-let"
        yield Request(start_url, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='eapow-property-thumb-holder']"):
            url = response.urljoin(item.xpath("./a/@href").get())
            rented = item.xpath(".//div[@class='eapow-bannertopright']").get()
            if rented:
                continue
            yield Request(url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
        
        next_page = response.xpath("//ul[@class='pagination-list']//li/a[.='Next']/@href").get()   
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type": response.meta.get('property_type')})
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source) 
        external_id=response.xpath("//b[contains(.,'Ref')]/following-sibling::text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split(":")[-1].strip()) 
        item_loader.add_xpath("title", "//h1/text()[normalize-space()]")     


        address = ", ".join(response.xpath("//div[@class='eapow-sidecol eapow-mainaddress']//text()").getall())
        if address:
            item_loader.add_value("address", address.strip())  
        city_zip = response.xpath("//div[@class='eapow-sidecol eapow-mainaddress']/address/text()").get()
        if city_zip:
            item_loader.add_value("zipcode", " ".join(city_zip.strip().split(" ")[-2:]))  
            city1=city_zip.strip().split(" ")[0].strip()
            a=re.search("[A-Z]+[A-Z-0].*",city1)
            if a:
                city=item_loader.get_output_value("title")
                item_loader.add_value("city",city.split(",")[-2])  
            else:
                city=city_zip.strip().split(" ")[0].strip()
                if city:
                    item_loader.add_value("city",city)

        f_text = "".join(response.xpath("//div[@id='propdescription']//text()[normalize-space()]").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            return              

        item_loader.add_xpath("room_count", "//div[i[@class='flaticon-bed']]/span/text()") 
        item_loader.add_xpath("bathroom_count", "//div[i[@class='flaticon-bath']]/span/text()") 
        rent = response.xpath("//small[@class='eapow-detail-price']//text()").get()
        if rent:
            if "week" in rent.lower():
                rent = rent.split("£")[-1].split(" ")[0].replace(",","").strip()
                item_loader.add_value("rent", str(int(rent)*4))
            else:
                item_loader.add_value("rent_string", rent)
        item_loader.add_value("currency", "GBP")

        description = " ".join(response.xpath("//div[@id='propdescription']//text()[normalize-space()]").getall())
        if description:
            item_loader.add_value("description", description.strip())
      
        images = [x for x in response.xpath("//div[@id='slider']//li/img/@data-src").getall()]
        if images:
            item_loader.add_value("images", images) 
        furnished=response.xpath("//ul//li[.='Furnished']").get()
        if furnished:
            item_loader.add_value("furnished",True)

        item_loader.add_value("landlord_phone", "02073788696")
        item_loader.add_value("landlord_email", "info@viewlettings.com")
        item_loader.add_value("landlord_name", "View lettings") 

        latitude_longitude = response.xpath("//script[@type='text/javascript']/text()[contains(.,'lat:')]").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat: "')[1].split('"')[0]
            longitude = latitude_longitude.split('lon: "')[1].split('"')[0]
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
      

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "bovenwoning" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "cottage" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"    
    else:
        return None