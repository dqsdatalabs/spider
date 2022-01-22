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
    name = 'demeester_eu'   
    execution_type='testing'
    country='belgium'
    locale='fr'  
    external_source="Demeester_PySpider_belgium_fr"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.demeester.eu/te-huur-antwerpen/type/appartement/",
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "https://www.demeester.eu/te-huur-antwerpen/type/huis/",
                ],
                "property_type" : "house"
            },   
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='inner']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        next_page = response.xpath("//i[contains(@class,'fa-chevron-right')]/../@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={'property_type': response.meta.get('property_type')})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "//div[@class='col-sm-12']/p/text()")

        address = " ".join(response.xpath("//div[contains(@class,'title')]/p[3]/text()").extract())
        if address:
           item_loader.add_value("address", re.sub("\s{2,}", " ", address.strip())) 

        city =  " ".join(response.xpath("//span[@class='address']/text()").extract())
        if city:
            item_loader.add_value("city", city.strip().split(" ")[-1])
            item_loader.add_value("zipcode", city.strip().split(" ")[0])

        meters = "".join(response.xpath("//div[@class='col-sm-7']/div/div/span[contains(.,'m²')]").extract())
        if meters:
            s_meters =  meters.split(".")[0]
            item_loader.add_value("square_meters", s_meters.strip().replace("m²",""))
        else:
            meters = response.xpath("//th[contains(.,'Bewoonbare opp')]/following-sibling::td/text()").get()
            if meters:
                item_loader.add_value("square_meters", meters.split('m')[0].strip())

        room = "".join(response.xpath("//div[@class='col-sm-4']/ul/li[span[.='Slaapkamers']]/span[2]/text()").extract())
        if room:
            item_loader.add_value("room_count", room.strip())

        bathroom = "".join(response.xpath("//div[@class='col-sm-4']/ul/li[span[.='Badkamers']]/span[2]/text()").extract())
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom.strip())

        price = "".join(response.xpath("substring-before(//div[@class='estate-specific']/span[@class='price']/text(),'/')").extract())
        if price:
            item_loader.add_value("rent_string", price.strip().replace(" ",""))
        else:
            item_loader.add_value("currency", "EUR")
        

        item_loader.add_xpath("external_id", "//div[@class='col-sm-4']/ul/li[span[.='Referentie nr.']]/span[2]/text()")

        utilities = response.xpath("//th[contains(.,'Maandelijkse kosten')]/following-sibling::td/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.strip())
        else:
            utilities = response.xpath("//div[@class='col-sm-4']/ul/li[span[.='Lasten / maand']]/span[2]/text()").get()
            if utilities:
                item_loader.add_value("utilities", utilities.strip())

        item_loader.add_xpath("energy_label", "//div[@class='col-sm-4']/ul/li[span[.='EPC']]/span[2]/text()")
        item_loader.add_xpath("floor", "//div[@class='col-sm-4']/ul/li[span[.='Verdieping']]/span[2]/text()")

        desc = " ".join(response.xpath("//div[contains(@class,'page-content')]//text()[not(contains(.,'Graag meer Info?') or contains(.,'Plan een bezichtiging') or contains(.,'Bekijk de virtuele tour'))]").extract())
        if desc:
            item_loader.add_value("description", desc)

        images=[x for x in response.xpath("//div[contains(@class,'estate')]/div/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))

        available_date=response.xpath("//div[@class='col-sm-4']/ul/li[span[.='Beschikbaarheid']]/span[2]/text()[not(contains(.,'onmiddellijk'))]").get()

        if available_date:
            
            date =available_date.replace("vanaf akte","").replace("af te spreken met eigenaar","") 
            if date  != "":
                date_parsed = dateparser.parse(
                    date, date_formats=["%m-%d-%Y"]
                )
                if date_parsed:
                    date3 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date3)

        elevator = "".join(response.xpath("//div[@class='col-sm-4']/ul/li[span[.='Lift']]/span[2]/text()").extract())
        if elevator:
            if "Ja" in elevator:
                item_loader.add_value("elevator", True)
            elif "Nee" in elevator:
                item_loader.add_value("elevator", False)

        terrace = "".join(response.xpath("//div[@class='col-sm-4']/ul/li[span[.='Terras']]/span[2]/text()").extract())
        if terrace:
            if "Ja" in terrace:
                item_loader.add_value("terrace", True)
            elif "Nee" in terrace:
                item_loader.add_value("terrace", False)

        parking = "".join(response.xpath("//div[@class='col-sm-4']/ul/li[span[.='Garages']]/span[2]/text()").extract())
        if parking:
            if "Ja" in parking:
                item_loader.add_value("parking", True)
            elif "Nee" in parking:
                item_loader.add_value("parking", False)
        else:
            parking = response.xpath("//span[contains(text(),'Garage') or contains(text(),'garage')]").get()
            if parking:
                item_loader.add_value("parking", True)

        item_loader.add_value("landlord_email", "INFO@DEMEESTER.EU")
        item_loader.add_value("landlord_name", "Demeester")
        item_loader.add_value("landlord_phone", "03 227 06 36")


        yield item_loader.load_item()
