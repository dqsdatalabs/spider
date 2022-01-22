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
    name = 'parkgate_info'
    execution_type='testing'
    country='united_kingdom'
    locale='en'  
    thousand_separator = ','
    scale_separator = '.' 
    custom_settings = {
        "PROXY_ON":True,
    }
    
    def start_requests(self):
        start_urls = [
            {
                "property_type" : "apartment",
                "type" : "Apartment"
            },
            {
                "property_type" : "apartment",
                "type" : "Flat"
            },
            {
                "property_type" : "house",
                "type" : "House"
            },
            {
                "property_type" : "house",
                "type" : "Semi-Detached"
            },
        ]
        for item in start_urls:
            formdata = {
                "area": "",
                "type": "lettings",
                "price_min": "",
                "price_max": "",
                "bedrooms_min": "",
                "PropertyType": item["type"],
            }
            yield FormRequest(
                "https://www.parkgate.info/property-search",
                callback=self.parse,
                formdata=formdata,
                #dont_filter=True,
                meta={
                    "property_type":item["property_type"],
                    "type":item["type"]
                    })

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 10)
        seen = False
        for item in response.xpath("//div[@class='thumbnail']"):
            status = item.xpath("./div/text()").get()
            if status and ("agreed" in status.lower() or status.strip().lower() == "let"):
                continue
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        
        if page == 10 or seen:
            p_type = response.meta["type"]
            formdata = {
                "fcount": f"area=&type=lettings&price_min=&price_max=&bedrooms_min=&PropertyType={p_type}&",
                "ptype": p_type,
                "paging": "10",
                "currpages": str(page),
            }
            url = "https://www.parkgate.info/more-properties"
            yield FormRequest(
                url,
                callback=self.parse,
                formdata=formdata,
                meta={
                    "property_type":response.meta["property_type"],
                    "page":page+10,
                    "type":p_type,
                })
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("id=")[1])
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", "Parkgate_PySpider_united_kingdom")
        
        title =response.xpath("//div/h1/text()").extract_first()
        if title:
            item_loader.add_value("title",title.strip() )   
       
        address =" ".join(response.xpath("//div/h1/text()").extract())
        if address:
            item_loader.add_value("address",address.strip())
            item_loader.add_value("city","Richmond")
            zipcode = address.split(",")[-1].strip()
            if not zipcode.replace(" ","").isalpha():
                item_loader.add_value("zipcode", zipcode)
         
        item_loader.add_xpath("rent_string","//span[@class='price']//text()")                
        
        available_date = response.xpath("substring-after(//section[@id='PropFeat']//ul/li[contains(.,'Available')]//text(),'Available')").extract_first() 
        if available_date:  
            date_parsed = dateparser.parse(available_date.strip(),date_formats=["%d-%m-%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        floor =response.xpath("substring-before(//section[@id='PropFeat']//ul/li[contains(.,'Floor')]//text(),'Floor')").extract_first()
        if floor:
            item_loader.add_value("floor",floor.strip() )  

        room_count = response.xpath("//section[@id='PropType']//p[contains(.,'Bedrooms')]/text()[.!=': 0']").extract_first() 
        if room_count:   
            item_loader.add_value("room_count",room_count.replace(":","") )
        
        energy_label = response.xpath("substring-after(//section[@id='PropFeat']//ul/li[contains(.,'EPC')]//text(),': ')").extract_first()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
        balcony = response.xpath("//section[@id='PropFeat']//ul/li[contains(.,'Balcony')]//text()").extract_first()
        if balcony:
            item_loader.add_value("balcony", True)
        furnished = response.xpath("//section[@id='PropFeat']//ul/li[contains(.,'Furnished') or contains(.,'furnished')]//text()").extract_first()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)
         
        parking = response.xpath("//section[@id='PropFeat']//ul/li[contains(.,'Parking ')]//text()").extract_first()
        if parking:
            item_loader.add_value("parking",True)    
            
        desc = " ".join(response.xpath("//section[@id='PropDesc']//p/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        images = [x for x in response.xpath("//section[@id='PropImages']//ul/li/img/@src").extract()]
        if images:
                item_loader.add_value("images", images)
        floor_plan_images = [response.urljoin(x) for x in response.xpath("//section[@id='PropButts']//ul/li[contains(.,'Floorplans')]/a/@href").extract()]
        if floor_plan_images:
                item_loader.add_value("floor_plan_images", floor_plan_images) 

        item_loader.add_value("landlord_name", "Parkgate Estates")
        item_loader.add_value("landlord_phone", "020 8940 2991")
        item_loader.add_value("landlord_email", "info@parkgate.info")    
        
        yield item_loader.load_item()
