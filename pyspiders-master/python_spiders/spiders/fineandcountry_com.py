# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider 
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin
import re
from word2number import w2n 
import dateparser

class MySpider(Spider):
    name = 'fineandcountry_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    start_urls = ["https://www.fineandcountry.com/uk/search?page=1&national=false&p_department=RL&p_division=&location=&auto-lat=&auto-lng=&keywords=&minimumPrice=&minimumRent=&maximumPrice=&maximumRent=&rentFrequency=&minimumBedrooms=0&maximumBedrooms=&searchRadius=&recentlyAdded=&propertyIDs=&propertyType=&rentType=&orderBy=price%2Bdesc&networkID=&clientID=&officeID=&availability=&propertyAge=&prestigeProperties=&includeDisplayAddress=Yes&videoettesOnly=0&360TourOnly=0&virtualTourOnly=0&country=&addressNumber=&equestrian=0&tag=&golfGroup=&coordinates=&priceAltered=&sfonly=0&openHouse=0&student=&language=en&limit=20"]

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[contains(.,'pcm')]/preceding-sibling::*"):
            f_url = response.urljoin(item.xpath("./@href").get())
            price = item.xpath("./@data-defaultprice").get()
            yield Request(
                f_url, 
                callback=self.populate_item,
                meta = {"price": price}
            )
        
        next_page = response.xpath("//a[.='›']/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
            )
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Fineandcountry_PySpider_" + self.country + "_" + self.locale)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-1].strip())
        rented = response.xpath("//div[@class='main_image']//span[.='Let Agreed']/text()").extract_first()
        if rented:
            return
        title = response.xpath("normalize-space(//h1[@class='text-left']/text())").get()
        if title and ("Apartment" in title or "Flat" in title):
            item_loader.add_value("property_type", "apartment")
        elif title and ("House" in title or "Cottage" in title or "Bungalow" in title or "Character Property" in title):
            item_loader.add_value("property_type", "house")
        elif title and "Studio" in title:
            item_loader.add_value("property_type", "studio")
        else:
            return
        
        title = response.xpath("//h1[@class='text-left']/text()").extract_first()
        if title:
            title = re.sub("\s{2,}", " ", title)
            item_loader.add_value("title", title)

        address = response.xpath("//span[contains(@class,'displayAddressTitle')]//text()").extract_first()
        if address:
            item_loader.add_value("address", address)

  
             
        # rent = response.meta.get("price") + "£"
        # if rent:
        #     item_loader.add_value("rent_string", rent.replace(",","."))   
        rent = response.meta.get("price")+"£"
        if rent:
             item_loader.add_value("rent_string", rent.split(".")[0].replace(",","."))

        item_loader.add_value("currency","GBP")
 

    
        room_count = response.xpath("//div[@class='h4 mb-20']/span[i[@class='fa fa-bed']]/text()").extract_first()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())

        bathroom_count = response.xpath("//i[contains(@class,'fa-bath')]/following-sibling::text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        available_date = response.xpath("//li[contains(text(),'Available from')]/text()").get()
        if available_date:
            available_date = available_date.split('from')[-1].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d %B %Y"], languages=['en'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
            
        deposit = response.xpath("//li[contains(text(),'Deposit')]/text()").get()
        if deposit:
            item_loader.add_value("deposit", str(int(float(deposit.split('£')[-1].split("p")[0].replace(',', '')))))

        utilities = response.xpath("//div[@class='full_description_large ul-default']//text()[contains(.,'charged')]").get()
        if utilities: 
            item_loader.add_value("utilities", utilities.replace(",",""))

        city_zip = response.xpath("//span[contains(@class,'displayAddressTitle')]/text()").get()
        if city_zip:
            if "- " in city_zip:
                city = city_zip.split('- ')[-1].strip()
 
            else: 
                city = city_zip.split(',')[-1].strip() 
         
            try:
                if not city.replace(" ","").isalpha():
                    city = city_zip.split(',')[-2].strip()
                    zipcode = city_zip.split(',')[-1].strip()
                    if zipcode and not "Manor Road" in zipcode:
                        if zipcode.count(" ")>1: zipcode = f"{zipcode.split(' ')[-2]} {zipcode.split(' ')[-1]}"
                        item_loader.add_value("zipcode", zipcode.strip())                
            except:
                pass
          
            item_loader.add_value("city", city.strip())


        energy_label = response.xpath("//li[contains(text(),'EPC Rating')]/text()").get()
        if energy_label:
            energy_label = energy_label.split('-')[-1].strip()
            if energy_label in ['A', 'B', 'C', 'D', 'E', 'F', 'G']:
                item_loader.add_value("energy_label", energy_label)

        desc = "".join(response.xpath("//div[@class='full_description_large ul-default']//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

            if 'floor' in desc.lower():
                try:
                    floor = w2n.word_to_num(desc.lower().split('floor')[0].strip().split(' ')[-1].strip())
                    item_loader.add_value("floor", str(floor))
                except:
                    pass
            if 'no pets' in desc.lower():
                item_loader.add_value("pets_allowed", False)
            elif 'allowing pets' in desc.lower():
                item_loader.add_value("pets_allowed", True)
            if 'parking' in desc.lower():
                item_loader.add_value("parking", True)
            if 'lift' in desc.lower():
                item_loader.add_value("elevator", True)
            if 'swimming pool' in desc.lower():
                item_loader.add_value("swimming_pool", True)
            if 'dishwasher' in desc.lower():
                item_loader.add_value("dishwasher", True)
            if 'washing machine' in desc.lower():
                item_loader.add_value("washing_machine", True)

        features = "".join(response.xpath("//div[@class='tab-body']//text()").extract())
        if features:
            if "furnished" in features:
                item_loader.add_value("furnished", True) 
            if "terrace" in features:
                item_loader.add_value("terrace", True) 
            if "balcon" in features:
                item_loader.add_value("balcony", True)   
        
        square_meters = response.xpath("//ul[@class='ul-default']/li[contains(.,'sq ft') or contains(.,'sqft')]//text()").extract_first()
        if square_meters:
            square_meters=square_meters.replace(",","")
            square_meters=re.findall("\d+",square_meters)
            item_loader.add_value("square_meters", square_meters)



            # unit_pattern = re.findall(r"[+-]? *((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*(sq ft|sqft)",square_meters.replace(",",""))
            # if unit_pattern:
            #     sqm = str(int(float(unit_pattern[0][0]) * 0.09290304))
            #     item_loader.add_value("square_meters", sqm)



        external_id = response.xpath("//div[@class='mb-10'][contains(.,'Ref')]/small/text()").extract_first()
        if external_id:
            item_loader.add_value("external_id", external_id.split("Ref:")[1].strip())       

        images = [x for x in response.xpath("//div[@id='thumbnail_images']//a/@href").extract()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        floor_plan_images = [x for x in response.xpath("//div[@id='floorplan']//img/@src").extract()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        coordinate = response.xpath("//script[contains(.,'initMap')]/text()").extract_first()
        if coordinate:
            latitude = coordinate.split('{lat:')[1].split(',')[0].strip()
            longitude = coordinate.split('lng:')[1].split('}')[0].strip()        
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)    

        item_loader.add_xpath("landlord_email", "//p/strong[contains(.,'E:')]/following-sibling::a//text()")
        item_loader.add_xpath("landlord_name", "//div[@class='panel-heading' and contains(.,'&')]/h2/text()")
        item_loader.add_xpath("landlord_phone", "//p/a[contains(@href,'tel')]//text()")

        yield item_loader.load_item()

