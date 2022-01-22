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
    name = 'bristolreslet_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source="Bristolreslet_PySpider_united_kingdom"
    def start_requests(self):
        yield Request("https://www.bristolreslet.com/properties/", callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//div[@class='col-md-12 prop_image']//a/@href").getall():
         
            seen = True
            yield Request(response.urljoin(item), callback=self.populate_item)
        
        if page == 2 or seen:
            follow_url = f"https://www.bristolreslet.com/properties?_sfm_price=500+5100&sf_paged={page}"
            yield Request(follow_url, callback=self.parse, meta={"page": page + 1})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title", "//title/text()")
        rent=response.xpath("//span[contains(.,'£')]/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent)
   
        room_count = response.xpath("//span[@class='title-text pp-primary-title']/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split('Bed')[0])
        bathroom_count = response.xpath("//ul//li//span[contains(.,'Bathrooms')]/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.split("Bath")[0].strip()
            if "three" in bathroom_count.lower():
                item_loader.add_value("bathroom_count", "3")
            if "two" in bathroom_count.lower():
                item_loader.add_value("bathroom_count", "2")
            if "one" in bathroom_count.lower():
                item_loader.add_value("bathroom_count", "1")

        available_date = response.xpath("//p[contains(.,'Available')]/text()").get()
        if available_date:  
            available_date=available_date.split(":")[-1]
            date_parsed = dateparser.parse(available_date.strip(),date_formats=["%d-%m-%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d")) 

        address = response.xpath("//div[@class='fl-rich-text']/h5/a/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("zipcode", address.split(",")[-1].strip())
            item_loader.add_value("city",address.split(",")[0])
        
        # latitude_longitude = response.xpath("//div[@class='map-sidebar breakout-tablet']/img/@src[not(contains(.,'center=0,0'))]").get()
        # if latitude_longitude:
        #     item_loader.add_value("longitude", latitude_longitude.split("?center=")[1].split(",")[0])
        #     item_loader.add_value("latitude", latitude_longitude.split("?center=")[1].split(",")[1].split("&")[0])
 
        desc="".join(response.xpath("//div[@itemprop='text']//text()").getall())
        if desc:
            desc=desc.replace("\n","").replace("\r","").replace("*","")
            item_loader.add_value("description", desc.strip())
        
      
        images=[x for x in response.xpath("//div[@class='pp-image-carousel-thumb']/@style").getall()]
        if images:
            img=[]
            for i in images:
                i=i.split(":url(")[-1].split(")")[0]
                img.append(i)
            item_loader.add_value("images", img)
        # floor = response.xpath("//ul/li/text()[contains(.,' Floor ')]").get()
        # if floor:
        #     item_loader.add_value("floor", floor.split(" Floor ")[0])
        furnished = response.xpath("//p[.='Furnished']/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        unfurnished=response.xpath("//p[.='Unfurnished']/text()").get()
        if unfurnished:
            item_loader.add_value("furnished", False)

        property_type1=response.xpath("//ul//li//span[contains(.,'Apartment')]/text()").get()
        if property_type1:
            item_loader.add_value("property_type","apartment")
        property_type2=response.xpath("//ul//li//span[contains(.,'Property')]/text()").get()
        if property_type2:
            item_loader.add_value("property_type","house")

        parking = response.xpath("//ul//li//span[contains(.,'Parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        balcony = response.xpath("//ul//li//span[contains(.,'Balcony')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        terrace = response.xpath("//ul//li//span[contains(.,'garden')]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        item_loader.add_value("landlord_name", "Clifton Team")
        item_loader.add_value("landlord_phone", "0117 9735237")
        item_loader.add_value("landlord_email", "clifton@bristolreslet.com")
               
        yield item_loader.load_item()
