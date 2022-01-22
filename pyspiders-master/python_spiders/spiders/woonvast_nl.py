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

class MySpider(Spider):
    name = 'woonvast_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    external_source = 'Woonvast_PySpider_netherlands'

    # 1. FOLLOWING
    def start_requests(self): 
        formdata = {
            "huistype": "galerijflat,portiekflat",
            "sorteer": "Desc~Datum",
            "prijs": "0,999999999",
            "prefilter": "Huuraanbod",
            "pagenum": "0",
            "pagerows": "12",
        }
        yield FormRequest(
            "https://www.woonvast.nl/huizen/smartselect.aspx",
            callback=self.jump,
            formdata=formdata,
            dont_filter=True,
            meta={
                "property_type":"apartment",
                "type" : "galerijflat,portiekflat"
            })
        formdata2 = {
            "huistype": "hoekwoning,tussenwoning",
            "sorteer": "Desc~Datum",
            "prijs": "0,999999999",
            "prefilter": "Huuraanbod",
            "pagenum": "0",
            "pagerows": "12",
        }
        yield FormRequest(
            "https://www.woonvast.nl/huizen/smartselect.aspx",
            callback=self.jump,
            formdata=formdata2,
            dont_filter=True,
            meta={
                "property_type":"house",
                "type" : "hoekwoning,tussenwoning"
            })
    def jump(self, response):
        data = json.loads(response.body)
        id_list = ""
        for i in data["AllMatches"]:
            id_list = id_list + i + ","
        
        formdata = {
            "id" : id_list.strip(",").strip(),
        }
        yield FormRequest(
            "https://www.woonvast.nl/huizen/smartelement.aspx",
            callback=self.parse,
            formdata=formdata,
            dont_filter=True,
            meta={
                "property_type": response.meta["property_type"],
                "type" : response.meta["type"],
                "pagi_info" : response.meta.get("pagi_info", 0)
            })
    

    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        pagi_info = response.meta.get("pagi_info", 0)
        for item in response.xpath("//div[@class='object-adres']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={
                    "property_type":response.meta["property_type"]})
            seen = True
        
        if page == 2 or seen:
            p_type = response.meta["type"]
            formdata = {
                "huistype": p_type,
                "sorteer": "Asc~Prijs",
                "prijs": "0,999999999",
                "prefilter": "Huuraanbod",
                "pagenum": str(page),
                "pagerows": "12",
            }
            yield FormRequest(
                "https://www.woonvast.nl/huizen/smartselect.aspx",
                callback=self.jump,
                formdata=formdata,
                meta={
                    "property_type":"apartment",
                    "type" : p_type,
                })
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        status = response.xpath("//div[@class='status status-rent-reserve']/span/text()").get()
        if status and ("onder" in status.lower() or "verhuurd" in status.lower()):
            return

        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        title = response.xpath("//h1/span[@class='adres']/text()").get()
        if title:
            item_loader.add_value("title", title)

        rent = response.xpath("//h2/text()").get()
        if rent:
            rent = rent.split('€')[-1].lower().split('p')[0].strip().replace('.', '').replace('\xa0', '')
            item_loader.add_value("rent", str(int(float(rent))))
        item_loader.add_value("currency", 'EUR')

        room_count = response.xpath("//div[contains(text(),'Aantal kamers')]/following-sibling::div/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//div[contains(text(),'Aantal badkamers')]/following-sibling::div/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        square_meters = response.xpath("//div[contains(text(),'Wonen')]/following-sibling::div/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split(' ')[0].split(',')[0].split('.')[0].strip())   
        
        external_id = response.url.split('-')[-1]
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        address = response.xpath("//h1/span[@class='adres']/text()").get()
        city = response.xpath("//h1/span[@class='plaatsnaam']/text()").get()
        if city:      
            item_loader.add_value("city", city.strip())
            if address: address += " " + city
        
        if address:
            item_loader.add_value("address", address.strip()) 
        
        parking = response.xpath("//div[contains(text(),'Faciliteit')]/following-sibling::div/text()[contains(.,'park')]").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//div[contains(text(),'ruimtes')]/following-sibling::div/text()[contains(.,'balkon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        description = " ".join(response.xpath("//div[@class='object-detail-description object-detail-description-small']//text()").getall()).strip()   
        if description:
            description = re.sub('\s{2,}', ' ', description.strip())
            item_loader.add_value("description", description)
        
        images = [response.urljoin(x) for x in response.xpath("//section[@id='object-all-photos']//img[@class='img-responsive img-fluid']//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        try:
            latitude = response.xpath("//input[contains(@id,'mgmMarker')]/@value").get()
            if latitude:
                item_loader.add_value("latitude", latitude.split('~')[2].split(',')[0].strip())
                item_loader.add_value("longitude", latitude.split('~')[2].split(',')[1].strip())
        except:
            pass

        item_loader.add_value("landlord_name", "Woonvast Makelaars")
        item_loader.add_value("landlord_phone", "033-4221010")
        item_loader.add_value("landlord_email", "info@woonvast.nl")
        
        yield item_loader.load_item()
