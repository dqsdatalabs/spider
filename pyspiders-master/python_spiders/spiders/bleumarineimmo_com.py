# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_number_only, format_date, remove_white_spaces, remove_unicode_char
from scrapy import Request,FormRequest
import re
from datetime import datetime
import lxml,js2xml

class BleuMarineImmo(scrapy.Spider):
    name = "bleumarineimmo_com"
    allowed_domains = ["bleumarineimmo.com"]
    execution_type = 'testing'
    country = 'france'
    locale ='fr'
    thousand_separator=','
    scale_separator='.'
    position = 0

    formdata = {
        '_nature_bien_1444983670_@!@0': '',
        'familly': '8',
        'search' : "rechercher",
        'engine' : '1'
    }


    def start_requests(self):
        
        type_list = [
            {
                "property_type": "apartment",
                "type":"APPARTEMENT"
            },
            {
                "property_type": "house",
                "type": "MAISON"
            },
            {
                "property_type": "studio",
                "type": "STUDIO"
            },
        ]
        for item in type_list:
            self.formdata["_nature_bien_1444983670_@!@0"] = item.get('type')
            yield FormRequest("http://bleumarine-immobilier.fr/location-de-vacances/", 
                            dont_filter=True, 
                            formdata=self.formdata,
                            callback=self.parse, 
                            meta={"property_type": item["property_type"], "type": item["type"]})

    def parse(self, response, **kwargs):
        
        for item in response.xpath("//article[contains(@class,'product')]"):
            url = item.xpath(".//a//@href").get()
            address = item.xpath(".//h1/text()").get()
            yield scrapy.Request(
                url = response.urljoin(url),
                callback = self.get_property_details,
                dont_filter = True,
                meta = {'request_url' : response.urljoin(url),'property_type':response.meta.get('property_type'),"address":address})

        next_page = response.xpath("//a[contains(@class,'pagerNext')]//@href").get()
        if next_page:
            self.formdata["_nature_bien_1444983670_@!@0"] = response.meta.get('type')
            yield FormRequest(
                response.urljoin(next_page), 
                dont_filter=True, 
                formdata=self.formdata,
                callback=self.parse, 
                meta={"property_type": response.meta.get("property_type"), "type": response.meta.get("type")})

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        external_link = response.meta.get('request_url')
        property_type = response.meta.get('property_type')
        address = response.meta.get('address')
        if address:
            item_loader.add_value("address", address.split(" - ")[1].strip())
            item_loader.add_value("city", address.split(" - ")[1].strip())
        
        item_loader.add_value("external_source", "BleuMarineImmo_PySpider_{}_{}".format(self.country, self.locale))
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('property_type',property_type)

        external_id = response.xpath("//h2//text()[contains(.,'Réf')]").get()
        if external_id:
            external_id = external_id.split("Réf.")[-1].strip()
            item_loader.add_value("external_id", external_id)
        
        title = " ".join(response.xpath("//h2//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        rent = response.xpath("//p[contains(@class,'prix')]/text()").extract_first()
        if rent:
            rent = rent.strip().split(" ")[-1].replace("€","")
            item_loader.add_value("rent", int(rent)*4)
            item_loader.add_value("currency", "EUR")

        square_meters = response.xpath("//li[contains(.,'Surface totale')]//span//text()").get()
        if square_meters:
            square_meters = square_meters.split("m")[0].strip()
            item_loader.add_value("square_meters", square_meters.strip())

        desc = " ".join(response.xpath("//div[contains(@class,'prod--text')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)      
        
        
        room_count = response.xpath("//li[contains(.,'pièce')]//span//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())

        bathroom_count = response.xpath("//li[contains(.,'salle')]//span//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip()
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath('//div[contains(@id,"prod--slider")]//img/@src').getall()]
        if images:
            item_loader.add_value("images", images)

        parking = response.xpath("//li[contains(.,'parking')]//span//text()[contains(.,'oui')]").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//li[contains(.,'Balcon')]//span//text()[contains(.,'oui')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//li[contains(.,'Terrasse')]//span//text()[contains(.,'oui')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        floor = response.xpath("//li[contains(.,'étage')]//span//text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
            
        dishwasher = response.xpath("//li[contains(.,'Lave vaisselle')]//text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)

        latitude_longitude = response.xpath("//script[contains(.,'getMarker')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('getMarker(')[1].split(',')[0]
            longitude = latitude_longitude.split('getMarker(')[1].split(',')[1].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "Bleu Marine Immobilier")
        item_loader.add_value("landlord_phone", "02 51 30 08 09")

        yield item_loader.load_item()