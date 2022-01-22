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
    name = 'trincalimmobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Trincalimmobilier_PySpider_france'

    def start_requests(self):
        start_urls = [
            {
                "url": "http://www.trincalimmobilier.com/locations/appartement-t1", 
                "property_type": "apartment"
            },
            {
                "url": "http://www.trincalimmobilier.com/locations/appartement-t2", 
                "property_type": "apartment"
            },
            {
                "url": "http://www.trincalimmobilier.com/locations/appartement-t3", 
                "property_type": "apartment"
            },
            {
                "url": "http://www.trincalimmobilier.com/locations/appartement-t4", 
                "property_type": "apartment"
            },
            {
                "url": "http://www.trincalimmobilier.com/locations/appartement-t5", 
                "property_type": "apartment"
            },
            {
                "url": "http://www.trincalimmobilier.com/locations/studios", 
                "property_type": "studio"
            },
            {
                "url": "http://www.trincalimmobilier.com/locations/maison-villa", 
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='grid-offer']"):
            url = item.xpath(".//div[@class='button']/a/@href").get()
            status = item.xpath(".//div[@class='transaction-type']/text()[.='Vente']").get()
            if status:
                continue
            prop_type = item.xpath(".//div[@class='estate-type']/text()[.='Garage']").get()
            if prop_type:
                continue
            yield Request(response.urljoin(url), callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

        next_button = response.xpath("//i[contains(@class,'pag-next')]/../@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse)
            
# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)    

        status_check = response.xpath("(//div[contains(@class,'details-title pull-left')]//h3//text())[1]").get()
        if status_check and "commercial" not in status_check.lower():
            item_loader.add_value("property_type", response.meta.get('property_type'))
            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_source", self.external_source)
            externalid=response.xpath("//link[@rel='shortlink']/@href").get()
            if externalid:
                item_loader.add_value("external_id",externalid.split("p=")[-1])
    
            city = response.xpath("//div[div[.='Vue']]/div[@class='details-parameters-val']/text()").get()
            if city:
                item_loader.add_value("city", city.strip())
            adres=response.xpath("//h3//text()").get()
            if adres:
                item_loader.add_value("address", adres.split("/")[-1])
            item_loader.add_xpath("title", "//div[contains(@class,'details-title')]/h3/text()")
            item_loader.add_xpath("zipcode", "//div[div[.='Code Postal']]/div[@class='details-parameters-val']/text()")
            item_loader.add_xpath("floor", "//div[div[.='Etage']]/div[@class='details-parameters-val']/text()")
            item_loader.add_xpath("rent_string", "//div[contains(@class,'details-parameters-price')]/text()")

            bathroom_count = response.xpath("//div[div[.='Salles de bains']]/div[@class='details-parameters-val']/text()").get()
            if bathroom_count:
                item_loader.add_value("bathroom_count", bathroom_count)
            else:
                bathroom_count = response.xpath("//div[contains(@class,'details-parameters-name')][contains(.,'salle')]//following-sibling::div//text()").get()
                if bathroom_count:
                    item_loader.add_value("bathroom_count", bathroom_count)
    
            description = " ".join(response.xpath("//div[@class='details-desc']//text()[normalize-space()]").getall())
            if description:
                item_loader.add_value("description", description.strip())

            available_date=response.xpath("//div[@class='details-desc']//div[contains(.,'Libre au')]//text()[normalize-space()]").get()
            if available_date:
                item_loader.add_value("available_date",available_date.split("Libre au")[1])

            room_count = response.xpath("//div[div[.='Chambres']]/div[@class='details-parameters-val']/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count)
            else:
                item_loader.add_xpath("room_count", "//div[div[.='Pièces']]/div[@class='details-parameters-val']/text()")

            square_meters = response.xpath("//div[div[.='Surface']]/div[@class='details-parameters-val']/text()").get()
            if square_meters:
                item_loader.add_value("square_meters", square_meters.split("m")[0].strip())
            deposit = response.xpath("//div[@class='details-parameters-cont'][div[contains(.,'Dépôt de')]]/div[2]/text()").get()
            if deposit:
                item_loader.add_value("deposit", deposit)
            utilities = response.xpath("//div[contains(.,'charges')]/following-sibling::div[@class='details-parameters-val'][1]/text()").get()
            if utilities:
                item_loader.add_value("utilities", utilities)
            utilitiescheck=item_loader.get_output_value("utilities")
            if not utilitiescheck:
                utilities1=response.xpath("//div[contains(.,'Charges')]/following-sibling::div[@class='details-parameters-val'][1]/text()").get()
                if utilities1:
                    item_loader.add_value("utilities",utilities1)

            images = [response.urljoin(x) for x in response.xpath("//div[@class='slide-bg swiper-lazy']/@data-background").getall()]
            if images:
                item_loader.add_value("images", images)
            script_map = response.xpath("//script[contains(.,'street_view')]/text()").get()
            if script_map:
                latlng = script_map.split('"street_view":"')[1].split('"')[0]
                if latlng:
                    item_loader.add_value("latitude", latlng.split(",")[0].strip())
                    item_loader.add_value("longitude", latlng.split(",")[1].strip())

            item_loader.add_value("landlord_name", "Trincal Immobilier")
            item_loader.add_value("landlord_phone", "04 71 09 55 76")
            yield item_loader.load_item()