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
    name = 'jeanoddy_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    thousand_separator = ','
    scale_separator = '.'
    
    def start_requests(self):

        formdata = {
            "search-bedrooms": "",
            "search-minprice": "1000",
            "search-maxprice": "10000",
            "property-search": "1",
        }

        yield FormRequest(
            url="https://www.jeanoddy.com/properties/",
            callback=self.parse,
            formdata=formdata,
        )

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='property-thumb-info']"):
            follow_url = response.urljoin(item.xpath(".//div[@class='property-thumb-info-image']/a/@href").get())
            address = item.xpath(".//div[@class='property-thumb-info-content']//address/text()").extract_first()
            room = item.xpath(".//ul/li[@title='Bedrooms']/text()").extract_first()
            bathroom = item.xpath(".//ul/li[@title='Bathrooms']/text()").extract_first()
            square_meters = item.xpath(".//ul/li[contains(.,'Area')]/text()").extract_first()
            rent = item.xpath(".//div[@class='property-thumb-info-image']/span[@class='property-thumb-info-label']//text()[normalize-space()]").extract_first()
            yield Request(follow_url, callback=self.populate_item,meta={'address': address,'room': room,'bathroom': bathroom,"rent":rent,"square_meters":square_meters})
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        externalid=response.xpath("//link[@rel='shortlink']/@href").get()
        if externalid:
            item_loader.add_value("external_id",externalid.split("p=")[-1]) 

        desc = "".join(response.xpath("//div[@class='pgl-detail']/p/text()").getall())
        if get_p_type_string(desc):
            item_loader.add_value("property_type", get_p_type_string(desc))
        else:
            return
 
        item_loader.add_value("external_source", "Jeanoddy_PySpider_united_kingdom")
         
        item_loader.add_xpath("title", "//h2/text()")
        
        rent = response.meta.get("rent")
        if rent:
            if "per week" in rent.lower():
                rent = rent.split('£')[1].lower().split(' per')[0].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent)) * 4))
                item_loader.add_value("currency", 'GBP')
            else:
                item_loader.add_value("rent_string", rent) 
        
        room_count = response.meta.get("room")
        if room_count:
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.meta.get("bathroom")
        if room_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        address = response.meta.get("address")
        zipcode = ""
        if address:
            item_loader.add_value("address", address)
            try:
                item_loader.add_value("city", address.split(",")[-2].strip())
                zipcode = address.split(",")[-1].strip()
            except:
                item_loader.add_value("city", address.split(" ")[0].strip())
                zipcode = address.split(" ")[-1].strip()
        if not address:
            address = response.xpath("//h2/text()").get()
            item_loader.add_value("address", address)          
            zipcode=len(address.split(","))
            if zipcode == 1:
                item_loader.add_value("city", address)
                zipcode = ""
            elif zipcode==2:
                a_city = address.split(",")[-1].strip()
                item_loader.add_value("city", a_city) 
                zipcode = ""
            else:
                zipcode = address.split(",")[-1].strip()
                a_city = address.split(",")[-2].strip()
                item_loader.add_value("city", a_city) 

        if zipcode and zipcode.strip():
            if zipcode.split(" ")[0].isalpha(): zipcode = zipcode.split(" ")[-1]
            
            if "London" not in zipcode:
                item_loader.add_value("zipcode", zipcode)

        square_meters = response.meta.get("square_meters")
        if square_meters:
            square_meters = square_meters.split("sq")[0].strip()
            sqm = str(int(float(square_meters.replace(",","")) * 0.09290304))
            item_loader.add_value("square_meters", sqm)
       
        desc = " ".join(response.xpath("//div[@id='description']//text()[normalize-space()]").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        lat = response.xpath("//div[@id='map']//div/@data-latitude").get()
        lng = response.xpath("//div[@id='map']//div/@data-longitude").get()
        if lat and lng:
            item_loader.add_value("latitude", lat.strip())
            item_loader.add_value("longitude", lng.strip())

        images = [x for x in response.xpath("//div[@class='superslides-pagination-container']//li/a/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)    

        floor_plan_images  = [x for x in response.xpath("//div[@id='floor-plan']//li//a/@href").extract()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images )    

        item_loader.add_value("landlord_phone", "0207 916 1116")
        item_loader.add_value("landlord_email", "info@jeanoddy.com")
        item_loader.add_value("landlord_name", "Jean Oddy & Co")   

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
