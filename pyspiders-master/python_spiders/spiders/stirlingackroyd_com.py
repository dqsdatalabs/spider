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
from word2number import w2n
import re
import dateparser

class MySpider(Spider):
    name = 'stirlingackroyd_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    
    def start_requests(self):

        start_urls = [
            {
                "url" : "https://www.stirlingackroyd.com/properties/lettings/tag-house,residential",
                "property_type" : "house"
            },
            {
                "url" : "https://www.stirlingackroyd.com/properties/lettings/tag-maisonette,residential",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.stirlingackroyd.com/properties/lettings/tag-new-home,residential",
                "property_type" : "house"
            },
            {
                "url" : "https://www.stirlingackroyd.com/properties/lettings/tag-flat,residential",
                "property_type" : "apartment"
            },
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[@class='property--hovering-link']/@href").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )

        next_page = response.xpath("//a[@rel='next']/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type" : response.meta.get("property_type")},
            )
           
        
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Stirlingackroyd_PySpider_"+ self.country + "_" + self.locale)
        item_loader.add_value("external_link", response.url)

        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))

        prop_type = response.xpath("//ul[@class='bullet']/li[contains(.,'apartment') or contains(.,'Apartment') or contains(.,'house') or contains(.,'House')]/text()").get()
        if prop_type:
            if "studio" in prop_type.lower():
                item_loader.add_value("property_type", "house")
            elif "apartment" in prop_type.lower():
                item_loader.add_value("property_type", "apartment")
            elif "house" in prop_type.lower():
                item_loader.add_value("property_type", "house")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
        external_id = response.url.split('/')[-2].strip()
        if external_id:
            item_loader.add_value("external_id", external_id)

        address = response.xpath("//h1//text()").get()
        if address:
            item_loader.add_value("address", address.split())
            city = address.split(',')[-2].strip()
            item_loader.add_value("city", city)
        zipcode=response.xpath("//script[contains(.,'setLocratingIFrameProperties')]/text()").get()
        if zipcode:
            zipcode = zipcode.split("setLocratingIFrameProperties")[-1].split('search')[-1].split(",")[0].replace("'","").replace(":","")
            item_loader.add_value("zipcode", zipcode)

        bathroom_count = "".join(response.xpath("//h2[contains(.,'bath')]/text()").getall()).strip()
        if bathroom_count:
            bathroom_count = bathroom_count.split('bath')[0].split('|')[-1].strip()
            if bathroom_count.isnumeric():
                item_loader.add_value("bathroom_count", bathroom_count)

        available_date = response.xpath("//li[contains(.,'Available from')]/text()").get()
        if available_date:
            available_date = available_date.lower().split('from')[-1].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d %B %Y"], languages=['en'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        floor = response.xpath("//ul[@class='bullet']/li[contains(.,'floor') or contains(.,'Floor')]/text()").get()
        if floor:
            floor = floor.lower().split("floor")[0].strip()
            if floor and ("wood" not in floor) and ("parquet" not in floor):
                if " " in floor:
                    item_loader.add_value("floor", floor.split(" ")[-1])
                else:
                    item_loader.add_value("floor", floor)
        
        floor_plan_images = [x for x in response.xpath("//div[@id='floorplan']//img/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        parking = response.xpath("//li[contains(.,'parking') or contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True)
        
        elevator = response.xpath("//li[contains(.,'lift') or contains(.,'Lift')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        terrace = response.xpath("//li[contains(.,'terrace') or contains(.,'Terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        room_count=" ".join(response.xpath("//h2[contains(.,'bed')]/text()").getall()).strip()
        room=room_count.split("|")[0]
        if "bed" in room:
            item_loader.add_value("room_count", room.split(" ")[0])
        else:            
            room=" ".join(response.xpath("//div[@class='property--content']/p/text()[contains(.,'studio') or contains(.,'STUDIO')]").getall()).strip()
            if room:
                item_loader.add_value("room_count", "1")
                
        rent="".join(response.xpath("//h2[contains(.,'pcm')]/text()").getall()).strip()
        price=rent.split("|")[-1].split("pcm")[0]
        if "(" in price:
            price=price.split("(")[1].replace(",","")
            item_loader.add_value("rent_string", price)
        elif price:
            item_loader.add_value("rent_string", price.replace(",",""))

        latitude_longitude = response.xpath("//script[contains(.,'LatLng')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(",")[0]
            longitude = latitude_longitude.split('LatLng(')[1].split(",")[1].split(")")[0]
            if longitude != '0.0':
                item_loader.add_value("longitude", longitude.strip())
            if latitude != '0.0':
                item_loader.add_value("latitude", latitude.strip())
        
        desc="".join(response.xpath("//div[@class='property--content']/p/text()").getall()).strip()
        sqmt=response.xpath("//ul[@class='bullet']/li[contains(.,'Sq')]/text()").get()
        sqm=""
        if sqmt:
            try:
                square_meter=sqmt.split("Sq")[0].strip().split(" ")[-1]
                sqm = str(int(int(square_meter)* 0.09290304))
            except ValueError:
                sqm=None
        elif "sq ft balcony" in desc:
            sqm=None
        elif "sq ft" in desc.lower():
            try:
                square_meters=desc.split("sq")[0].strip().split(" ")[-1].replace("(","")
                sqm = str(int(int(square_meters)* 0.09290304))
            except ValueError:
                sqm=None
        if sqm:
            item_loader.add_value("square_meters", sqm)

        item_loader.add_value("description", re.sub("\s{2,}", " ", desc))

        images = [x for x in response.xpath("//div[@class='rsContent']/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
            
        name = response.xpath("//div[@class='branch--info']/p[contains(@class,'title')]//text()").get()
        if name:
            item_loader.add_value("landlord_name", name)

        furnished = "".join(response.xpath("//ul[@class='bullet']/li[contains(.,'Furnished') and not(contains(.,'Unfurnished'))]/text()").extract())
        if furnished :
            item_loader.add_value("furnished", True)       
        else:            
            unfurnished = response.xpath("//ul[@class='bullet']/li[contains(.,'Unfurnished') and not(contains(.,'Furnished'))]/text()").extract_first()
            if unfurnished :
                item_loader.add_value("furnished", False) 

        balcony = "".join(response.xpath("//ul[@class='bullet']/li[contains(.,'Balcony')]/text()").extract())
        if balcony :
                item_loader.add_value("balcony", True) 

        swimming_pool = "".join(response.xpath("//ul[@class='bullet']/li[contains(.,'swimming pool')]/text()").extract())
        if swimming_pool :
                item_loader.add_value("swimming_pool", True)   

        phone="+".join(response.xpath("//div[@class='branch--info']/p[contains(@class,'phone')]/a/text()").getall()).strip()
        if phone:
            item_loader.add_value("landlord_phone", phone.split("+")[0])
        item_loader.add_value("landlord_email","info@townends.co.uk")
        
        yield item_loader.load_item()

