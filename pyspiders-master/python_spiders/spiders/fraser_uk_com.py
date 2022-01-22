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
import dateparser
from word2number import w2n
from ..helper import extract_number_only

class MySpider(Spider):
    name = 'fraser_uk_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    
    
    def start_requests(self):

        start_urls = [
            {
                "url" : "https://www.fraser.uk.com/search.ljson?channel=lettings&fragment=tag-house/page-1",
                "property_type" : "house"
            },
            {
                "url" : "https://www.fraser.uk.com/search.ljson?channel=lettings&fragment=tag-apartment/page-1",
                "property_type" : "apartment"
            },
            

        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type'), "json_url":url.get("url")})


    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)

        data = json.loads(response.body)
        if data["properties"]:
            for item in data["properties"]:
                f_url = response.urljoin(item["property_url"])
                lat, lng = item["lat"], item["lng"]
                yield Request(
                    f_url, 
                    callback=self.populate_item, 
                    meta={"property_type" : response.meta.get("property_type"), "lat":lat, "lng":lng,},
                )

            json_url = response.meta.get("json_url")
            p_url = json_url.split("page")[0] + f"page-{page}"
            yield Request(
                url=p_url,
                callback=self.parse,
                meta={
                    "property_type" : response.meta.get("property_type"),
                    "page" : page+1,
                    "json_url" : json_url,
                }
            )
        
    # 2. SCRAPING level 2
    def populate_item(self, response):

        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Fraser_PySpider_" + self.country + "_" + self.locale)
        
        externalid=response.url
        if externalid:
            item_loader.add_value("external_id",externalid.split("properties/")[-1].split("/")[0])


        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        
        prop_type = response.xpath("//li/span[@itemprop='name']/text()").get()
        if "apartment" in prop_type.lower():
            item_loader.add_value("property_type", "apartment")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))

        item_loader.add_value("external_link", response.url)
        
        rent=response.xpath("//p[@class='price']/span/text()").get()
        if rent:
            if "week" in rent.lower():
                rent_number = re.findall(r'\d+(?:\.\d+)?', rent.replace(",","."))
                if rent_number:
                   rent = int(rent_number[0].replace(".",""))*4
                   item_loader.add_value("rent", rent)
                   item_loader.add_value("currency", "GBP")
            else:
                item_loader.add_value("rent_string", rent)
        
        desc = response.xpath("//div[contains(@class,'article-entry')]/p/text()").get()
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc.strip()))
        
        square_meters=response.xpath("//ul/li[contains(.,'Sq')]/text()").get()
        if square_meters and "SqFt" in square_meters :
            square_meters=square_meters.replace(",","").split("SqFt")[0].strip().split(" ")[-1]
            sqm=str(int(float(square_meters)* 0.09290304))
            item_loader.add_value("square_meters", sqm)
        elif square_meters  and "Sq" in square_meters:
            square_meters=extract_number_only(square_meters)
            sqm=str(int(float(square_meters)* 0.09290304))
            item_loader.add_value("square_meters", sqm)
        else:
            try:
                unit_pattern = re.findall(r"[+-]? *((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*(sq.ft.|sq ft|Sq Ft|SQ FT|sqft|sq|Sq|sq. ft|square feet)",desc.replace(",",""))
                if unit_pattern:
                    sqm=str(int(float(unit_pattern[0][0])* 0.09290304))
                    item_loader.add_value("square_meters", sqm)
            except: pass

        squaremetercheck=item_loader.get_output_value("square_meters")
        if not squaremetercheck:
            squ= "".join(response.xpath("//ul/li[contains(.,'sq')]/text()[normalize-space()]").getall())
            if squ:
                sqm = extract_number_only(squ)
                if "sq ft" in squ.lower():
                    item_loader.add_value("square_meters", int(float(sqm) * 0.09290304))
                else:
                    item_loader.add_value("square_meters", sqm)
                # square_meters=squ.replace(",","").split("sq")[0].replace("Approx","").replace(".","").replace("Spacious -","").strip()
                # sqm=str(int(float(square_meters)* 0.09290304))
                # item_loader.add_value("square_meters", sqm)
                    item_loader.add_value("square_meters", extract_number_only(squ))
        parking=response.xpath("//ul/li[contains(.,'Parking')]/text()").get()
        if parking:
            item_loader.add_value("parking",True)


        
        if desc and ("Available from" in desc):
            available_date = desc.split("Available from")[1].strip().replace(".","")
            if available_date:
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        
        latitude_longitude=response.xpath("//script[@type='text/javascript'][contains(.,'lat')]/text()").get()
        if latitude_longitude:
            latitude=latitude_longitude.split("lat:")[1].split(",")[0].strip()
            longitude=latitude_longitude.split("lng:")[1].split("}")[0].strip()

            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        adress=response.xpath("//h1/text()").get()
        if adress:
            item_loader.add_value("address", adress)
        
        zipcode="".join(response.xpath("//script[contains(.,'Ctesius.appendConfig') or contains(.,'postcode')]/text()").getall())
        if zipcode:
            zipcode=zipcode.split('"postcode":"')[1].split('"')[0]
            item_loader.add_value("zipcode",zipcode.strip())

        city = response.xpath("//h1/text()").get()
        if city:
            item_loader.add_value("city", city.split(',')[-2].strip())
            
        if desc and "dishwasher" in desc.lower():
            item_loader.add_value("dishwasher", True)

        room_count="".join(response.xpath("//ul[contains(@class,'list-features-teritary')]/li[contains(.,'Bedroom')]/text()").getall())
        if room_count:
            item_loader.add_value("room_count", room_count.strip().split(" ")[0])    
        else:
            if "studio" in desc.lower():
                item_loader.add_value("room_count", "1")

        bathroom_count="".join(response.xpath("//ul[contains(@class,'list-features-teritary')]/li[contains(.,'Bathroom')]/text()").getall())
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip().split(" ")[0])   
        else:
            bathroom_count = response.xpath("//li[contains(.,'Bathroom')]/text()").get()
            if bathroom_count:
                try:
                    item_loader.add_value("bathroom_count",w2n.word_to_num(bathroom_count.split(" ")[0]))
                except: pass
            
        images=[x for x in response.xpath("//div[contains(@class,'slider-thumbs')]/li/img/@data-img-src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        item_loader.add_value("landlord_name","FRASER")
        item_loader.add_value("landlord_phone","44 020 7723 5645")
        
        floor=response.xpath("//ul[@class='list-bullets']/li[contains(.,'Floor')]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.split("th")[0])
            
        balcony=response.xpath("//ul[@class='list-bullets']/li[contains(.,'Balcony')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        furnished = response.xpath("//ul[@class='list-bullets']/li[contains(.,'Furnished')]/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        elevator=response.xpath("//ul[@class='list-bullets']/li[contains(.,'Lift')]/text()").get()
        if elevator:
            item_loader.add_value("elevator",True)
        
        terrace=response.xpath("//ul[@class='list-bullets']/li[contains(.,'Terrace')]/text()").get()
        if terrace:
            item_loader.add_value("terrace",True)
            
        swimming_pool=response.xpath("//ul[@class='list-bullets']/li[contains(.,'Swimming pool')]/text()").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool",True)
        
        yield item_loader.load_item()