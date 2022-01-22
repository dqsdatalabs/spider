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
import re
from word2number import w2n
class MySpider(Spider):
    name = 'grandestateslondon_com'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.' 
    external_source = 'Grandestateslondon_PySpider_united_kingdom'
    def start_requests(self):
        start_urls = [
            {
                "property_type" : "apartment",
                "type" : "15354"
            },
            {
                "property_type" : "house",
                "type" : "15352"
            },
            {
                "property_type" : "house",
                "type" : "15353"
            },
            {
                "property_type" : "apartment",
                "type" : "15355"
            },
            {
                "property_type" : "house",
                "type" : "15356"
            },
            {
                "property_type" : "studio",
                "type" : "15357"
            },
            {
                "property_type" : "house",
                "type" : "15358"
            },
            {
                "property_type" : "room",
                "type" : "15682"
            },
        ]
        for item in start_urls:
            formdata = {
                "beds": "0",
                "min": "0",
                "max": "0",
                "type": item["type"],
                "location": "",
                "search": "",
            }
            yield FormRequest(
                "https://grandestates.domus.net/site/go/search?sales=false&items=12&includeUnavailable=false",
                callback=self.parse,
                formdata=formdata,
                #dont_filter=True,
                meta={
                    "property_type":item["property_type"],
                    "type":item["type"]
                })

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='searchResultPhoto']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        
        if page == 2 or seen:
            p_type = response.meta["type"]
            p_url = f"https://grandestates.domus.net/site/go/search?sales=false&min=0&includeUnavailable=false&beds=0&items=12&type={p_type}&max=0&location=&search=&page={page}&up=true"
            yield Request(
                p_url,
                callback=self.parse,
                meta={
                    "property_type":response.meta["property_type"],
                    "page":page+1,
                    "type":p_type,
                })
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", self.external_source)      
        title =" ".join(response.xpath("//h1//text()").extract())
        if title:
            item_loader.add_value("title",re.sub("\s{2,}", " ", title) )

        ext_id = response.url.split("propertyID=")[1].strip()
        if ext_id:
            item_loader.add_value("external_id", ext_id)
        
        address =response.xpath("//h1/span[@class='propertyAddress']/text()").extract_first()
        if address:
            item_loader.add_value("address",address.strip() ) 
            item_loader.add_value("city", address.split(",")[-2].strip())
            item_loader.add_value("zipcode", address.split(",")[-1].strip())
         
        item_loader.add_xpath("rent_string","//h1/span[@id='price']/text()")    
        square_meters = response.xpath("//div[@id='particularsSummary']//li[contains(.,'sq ft')]/span/text()").extract_first()
        if square_meters:
            unit_pattern = re.findall(r"[+-]? *((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*(sq ft|Sq. Ft.|sq. ft.|Sq. ft.|sq. Ft.|sq|Sq)",square_meters.replace(",",""))
            if unit_pattern:
                square_title=unit_pattern[0][0]
                sqm = str(int(float(square_title) * 0.09290304))
                item_loader.add_value("square_meters", sqm)            
        
        available_date = response.xpath("substring-after(//div[@id='particularsSummary']//li[contains(.,'Available')]/span/text(),'Available')").extract_first() 
        if available_date:  
            date_parsed = dateparser.parse(available_date.strip(),date_formats=["%d-%m-%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        bathroom = response.xpath("substring-before(//div[@id='particularsSummary']//li[contains(.,' Bathroom')]/span/text(),'Bathroom')").extract_first()
        if bathroom:            
            bathroom_count = w2n.word_to_num(bathroom.strip() )
            if bathroom_count:
                item_loader.add_value("bathroom_count",bathroom_count)
        if not item_loader.get_collected_values("bathroom_count"):
            bathroom = response.xpath("//span[contains(text(),'bathroom')]/text()").get()
            if bathroom:
                bathroom_count = w2n.word_to_num(bathroom.strip().split(" ")[0].strip())
                if bathroom_count:
                    item_loader.add_value("bathroom_count",bathroom_count)

        room_count = response.xpath("//h1/span[@class='bedroomsType']/text()[not(contains(.,'Studio'))]").extract_first() 
        if room_count:   
            item_loader.add_value("room_count",room_count.split("Bedroom")[0] )
        elif "studio" in response.meta["property_type"]:
            item_loader.add_value("room_count","1")
              
        balcony = response.xpath("//div[@id='particularsSummary']//li[contains(.,'balcony') or contains(.,'Balcony')]/span/text()").extract_first()
        if balcony:
            item_loader.add_value("balcony", True)


        furnished = response.xpath("//div[@id='particularsSummary']//li[contains(.,'furnished') or contains(.,'Furnished')]/span/text()").extract_first()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)
         
        elevator = response.xpath("//div[@id='particularsSummary']//li[span[.='Lift']]/span/text()").extract_first()
        if elevator:
            item_loader.add_value("elevator",True)    
            
        parking = response.xpath("//div[@id='particularsSummary']//li[contains(.,'Parking') or contains(.,'parking')]/span/text()").extract_first()
        if parking:
            item_loader.add_value("parking",True)                
        desc = " ".join(response.xpath("//div[@id='description']/h2[contains(.,'Summary')]/following-sibling::p[1]//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        images = [response.urljoin(x) for x in response.xpath("//div[@id='slideshow']//div[@class='sp-slide']/img/@src").extract()]
        if images:
                item_loader.add_value("images", images)
    
        script_map = response.xpath("//script/text()[contains(.,'L.marker([')]").get()
        if script_map:
            latlng = script_map.split("L.marker([")[1].split("],")[0]
            item_loader.add_value("latitude", latlng.split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split(",")[1].strip())

        item_loader.add_value("landlord_name", "Grand Estates")
        item_loader.add_value("landlord_phone", "0207 228 5922")
        item_loader.add_value("landlord_email", "info@grandestateslondon.com")           
        
        yield item_loader.load_item()
