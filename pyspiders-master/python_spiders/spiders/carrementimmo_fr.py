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
    name = 'carrementimmo_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Carrementimmo_PySpider_france"
    custom_settings = {"HTTPCACHE_ENABLED": False}
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.carrementimmo.fr/property_buyorrent/louer/#headeranchor",
                ],
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse)
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen = False
        for item in response.xpath("//a[@class='btn btn-lightgray']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        if page==2 or seen:
            nextpage=f"https://www.carrementimmo.fr/property_buyorrent/louer/page/{page}/"
            if nextpage:
                yield Request(nextpage, callback=self.parse,meta={"page":page+1})

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        property_type=response.xpath("//title//text()").get()
        if property_type:
            if "Villa" in property_type:
                item_loader.add_value("property_type","house")
            if "appartement" in property_type.lower():
                item_loader.add_value("property_type","apartment")
        dontallow=response.url
        if dontallow and "parking" in dontallow.lower():
            return 
        
        dontallow=response.xpath("//title//text()").get()
        if dontallow and "local" in dontallow:
            return
        adres=response.xpath("//h3[@class='detailpagesubheading']/text()").get()
        if adres:
            item_loader.add_value("address",adres.split("(")[0].strip())
        city=response.xpath("//h3[@class='detailpagesubheading']/text()").get()
        if city:
            item_loader.add_value("city",city.split(" ")[1].replace("\t","").replace("\n",""))
        zipcode=response.xpath("//h3[@class='detailpagesubheading']/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.split(" ")[0].replace("\t","").replace("\n",""))
        external_id=response.xpath("//h3[@class='detailpagesubheading']/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split("(")[1].split("#")[1].split(")")[0].strip())
        description=response.xpath("//div[@id='listingcontent']/p/text()").getall()
        if description:
            item_loader.add_value("description",description)
        rent=response.xpath("//h2[@id='pricebig']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0].strip())
        item_loader.add_value("currency","EUR")
        utilities=response.xpath("//li[contains(.,'Charges')]/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split(":")[1].split("€")[0].strip())
        room_count=response.xpath("//li[contains(.,'Pièces')]/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split(":")[1].strip())
        bathroom_count=response.xpath("//li[contains(.,'SDB')]/text()").get()
        if bathroom_count:
            bathroom_count=bathroom_count.split(":")[1].strip()
            if not "0" in bathroom_count:
                item_loader.add_value("bathroom_count",bathroom_count)
        square_meters=response.xpath("//li[contains(.,'Surface')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(":")[1].split("m")[0].split(",")[0].strip())
        energy_label=response.xpath("//span[@class='dpe230']//text()").get()
        if energy_label:
            energy = energy_label.replace("(","").replace(")","")
            item_loader.add_value("energy_label",energy_label_calculate(int(float(energy.replace(",",".")))))
        images=[x for x in response.xpath("//li//img//@src").getall()]
        if images:
            item_loader.add_value("images",images)
        item_loader.add_value("landlord_name","Agence Carrément Immo")
        item_loader.add_value("landlord_phone"," 09 50 50 00 21")
        item_loader.add_value("landlord_email","carrement.immo@gmail.com")

        yield item_loader.load_item()



def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number <= 50:
        energy_label = "A"
    elif energy_number > 50 and energy_number <= 90:
        energy_label = "B"
    elif energy_number > 90 and energy_number <= 150:
        energy_label = "C"
    elif energy_number > 150 and energy_number <= 230:
        energy_label = "D"
    elif energy_number > 230 and energy_number <= 330:
        energy_label = "E"
    elif energy_number > 330 and energy_number <= 450:
        energy_label = "F"
    elif energy_number > 450:
        energy_label = "G"
    return energy_label