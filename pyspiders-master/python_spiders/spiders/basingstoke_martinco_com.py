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
    name = 'basingstoke_martinco_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    thousand_separator = ','
    scale_separator = '.' 
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.martinco.com/property?p=1&per-page=24&intent=rent&price-per=pcm&type=flats_apartments&sort-by=price-desc",
                    "https://www.martinco.com/property?p=1&per-page=24&intent=rent&price-per=pcm&type=house_flat_share&sort-by=price-desc"
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.martinco.com/property?p=1&per-page=24&intent=rent&price-per=pcm&type=houses&sort-by=price-desc",
                    "https://www.martinco.com/property?p=1&per-page=24&intent=rent&price-per=pcm&type=bungalows&sort-by=price-desc",
                    
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.martinco.com/property?p=1&per-page=24&intent=rent&price-per=pcm&type=student&sort-by=price-desc",
                    
                ],
                "property_type" : "student_apartment"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base_url":item})


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//a[@class='property-link']"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        
        if page == 2 or seen:
            base_url = response.meta["base_url"]
            p_url = base_url + f"&p={page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1, "property_type":response.meta["property_type"], "base_url":base_url})
         
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response) 
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))    
        item_loader.add_value("external_source", "Basingstoke_Martinco_PySpider_united_kingdom") 
        dontallow=response.xpath("//strong[@id='propertyPrice']/span/text()").get()
        if dontallow and "unavailable" in dontallow.lower():
            return 
        dontallow2=response.xpath("//h2[@class='text-tertiary']/text()").get()
        if dontallow2 and "garages" in dontallow2.lower():
            return 


        item_loader.add_value("external_id", response.url.split('/')[-1].strip())    
   
        title = response.xpath("//h2[@class='text-tertiary']//text()").extract_first()
        if title:
            item_loader.add_value("title", re.sub("\s{2,}", " ", title)) 
        address = response.xpath("//div/h2[@class='text-secondary']//text()[.!='Key features']").extract_first()
        if address:
            item_loader.add_value("address",address.strip())     
            address = address.split(",")[-1].strip()        
            
        city_zipcode = response.xpath("//script[contains(.,'addresslocality')]//text()").get()
        if city_zipcode:
            city = city_zipcode.split('addresslocality": "')[1].split('"')[0]
            zipcode = city_zipcode.split('postalcode": "')[1].split('"')[0]
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)

        rent = " ".join(response.xpath("//div/h2//text()[contains(.,'£') and contains(.,'pm')]").extract())
        if rent:       
            rent = rent.split("pm")[0].split("£")[-1].strip().replace(",","")
            item_loader.add_value("rent", rent) 
        else:
            rent = " ".join(response.xpath("//div/h2//text()[contains(.,'£') and contains(.,'pcm')]").extract())
            if rent: 
                rent = rent.split("pcm")[0].split("£")[-1].strip().replace(",","")
                item_loader.add_value("rent", rent) 
        item_loader.add_value("currency", "GBP")
 
        item_loader.add_xpath("deposit","//div[@id='propertyFeatureList']//li[contains(.,'Deposit') and contains(.,'£')]//text()" ) 

        room_count = response.xpath("//h2[@class='text-tertiary']//text()[contains(.,'Bedroom')]").extract_first()
        if room_count:
            item_loader.add_value("room_count", room_count.split("Bedroom")[0])
        else:
            room_count = response.xpath("//h2[@class='text-tertiary']//text()[contains(.,'Studio')]").extract_first()
            if room_count:
                item_loader.add_value("room_count", "1") 
        
        bathroom_count = response.xpath("//div[contains(@id,'propertyFeatureList')]//li[contains(.,'Bathroom') or contains(.,'bathroom')]//text()").get()
        if bathroom_count:
            if "three" in bathroom_count.split(" ")[0].strip().lower():
                item_loader.add_value("bathroom_count","3")
            if "two" in bathroom_count.split(" ")[0].strip().lower() or "two" in bathroom_count.split("-")[0].strip().lower():
                item_loader.add_value("bathroom_count","2")
            if "one" in bathroom_count.split(" ")[0].strip().lower():
                item_loader.add_value("bathroom_count","1")
            else:
                item_loader.add_value("bathroom_count", bathroom_count.split(" ")[0])
        
        bathroomcountcheck=item_loader.get_output_value("bathroom_count")
        if not bathroomcountcheck:
            bathroom_count=response.xpath("//div[contains(@id,'propertyFeatureList')]//li[contains(.,'Bath')]//text()").get()
            if bathroom_count:
                item_loader.add_value("bathroom_count",bathroom_count.split("/")[-1].split("Bath")[0].strip())
        
        balcony = response.xpath("//div[@id='propertyFeatureList']//li[contains(.,'Balcony')]//text()").extract_first()
        if balcony:
            item_loader.add_value("balcony", True)
        available_date = " ".join(response.xpath("//div[@id='propertyFeatureList']//li[contains(.,'Available ')]//text()").extract())
        if available_date:  
            available_date = available_date.split('Available')[1].strip()
            if ":" in available_date:
                available_date = available_date.split(':')[1].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d-%m-%Y"], languages=['en'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        available=item_loader.get_output_value("available_date")
        if not available:
            available_date = " ".join(response.xpath("//div[@id='propertyFeatureList']//li[contains(.,'Available ')]//text()").extract())
            if available_date:  
                available_date = available_date.split('From')[-1].strip()
                if ":" in available_date:
                    available_date = available_date.split(':')[-1].strip()
                date_parsed = dateparser.parse(available_date, date_formats=["%d-%m-%Y"], languages=['en'])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

      
        furnished = response.xpath("//div[@id='propertyFeatureList']//li[contains(.,'Furnishing')]//text()").extract_first()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)
        floor  = response.xpath("substring-before(//div[@id='propertyFeatureList']//li[contains(.,'Floor')]//text(),'Floor')").extract_first()
        if floor :
            item_loader.add_value("floor", floor.strip())
        pets_allowed  = response.xpath("//div[@id='propertyFeatureList']//li[contains(.,'Pets')]//text()").extract_first()
        if pets_allowed :
            if "no" in pets_allowed.lower():
                item_loader.add_value("pets_allowed", False)
            else:
                item_loader.add_value("pets_allowed", True)
          
        parking = response.xpath("//div[@id='propertyFeatureList']//li[contains(.,'Parking')]//text()").extract_first()
        if parking:
            
            if " no " in parking.lower():
                item_loader.add_value("parking", False) 
            else:
                item_loader.add_value("parking", True)              
        desc = " ".join(response.xpath("//div[contains(@class,'property-description')]//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
        dontallow3=item_loader.get_output_value("description")
        if dontallow3 and "commercial" in dontallow3.lower():
            return 
    
        script_map = response.xpath("//script[contains(.,'marker = L.marker([')]//text()").get()
        if script_map:
            item_loader.add_value("latitude", script_map.split("marker = L.marker([")[1].split(",")[0].strip())
            item_loader.add_value("longitude", script_map.split("marker = L.marker([")[1].split(",")[1].split("]")[0].strip())

        images = [x for x in response.xpath("//div[@class='gallery']/div[@id='animated-thumbnails']/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)
    
        item_loader.add_value("landlord_name", "Martin & Co Basingstoke")
        landlord_phone = response.xpath("//a[contains(@href,'tel')]//p/text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)
        landlord_email = response.xpath("//a[contains(@href,'mailto')]//p//text()").get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email)

        yield item_loader.load_item()