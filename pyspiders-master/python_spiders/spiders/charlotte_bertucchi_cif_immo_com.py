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
from datetime import datetime
from python_spiders.helper import ItemClear
import re
 
class MySpider(Spider):
    name = 'charlotte_bertucchi_cif_immo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.cif-immo.com/ajax/ListeBien.php?page={}&TypeModeListeForm=pict&ope=2&filtre=2&lieu-alentour=0&langue=fr&MapWidth=100&MapHeight=0&DataConfig=JsConfig.GGMap.Liste&Pagination=0",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.cif-immo.com/ajax/ListeBien.php?page={}&TypeModeListeForm=pict&ope=2&filtre=8&lieu-alentour=0&langue=fr&MapWidth=100&MapHeight=0&DataConfig=JsConfig.GGMap.Liste&Pagination=0",    
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base":item})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        total_page = response.xpath("//span[contains(@class,'nav-page-position')]/text()").get()
        if total_page:
            total_page = int(total_page.split("/")[-1].strip())
        else:
            total_page = 1
        for item in response.xpath("//div[contains(@class,'liste-bien-photo ')]/div[1]/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        if page <= total_page:
            base = response.meta["base"]
            p_url = base.format(page)
            yield Request(p_url, callback=self.parse, meta={"property_type":response.meta["property_type"], "base":base, "page":page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Charlotte_Bertucchi_Cif_Immo_PySpider_france")      
        item_loader.add_xpath("title", "//h1/text()")
        external_id = response.xpath("//div[span[.='Ref']][last()]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        room_count = response.xpath("//li[contains(.,'chambre')]/text()[.!=' NC chambre(s)']").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split("ch")[0])
        else:
            room_count = response.xpath("//li[contains(.,' pièce')]/text()[.!=' NC pièce(s)']").get()
            if room_count:
                item_loader.add_value("room_count", room_count.split("pièce")[0])

        square_meters = response.xpath("//li[span[svg[contains(@class,'icon icon-surface')]]]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].strip())
    
        rent ="".join(response.xpath("//div[contains(@class,'detail-bien-prix')]/text()").getall())
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ",""))
        
        address = response.xpath("//h2[@class='detail-bien-ville']/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            zipcode = address.split("(")[-1].split(")")[0].strip()
            city = address.split("(")[0].strip()
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
        
        desc = "".join(response.xpath("//div[contains(@class,'detail-bien-desc-content')]/p[1]//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        latitude = response.xpath("//li[@class='gg-map-marker-lat']/text()[.!='0']").get()
        longitude = response.xpath("//li[@class='gg-map-marker-lng']/text()[.!='0']").get()
        if latitude or longitude:
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        images = [x for x in response.xpath("//div[@class='big-flap-container']/div/img/@data-src").getall()]
        if images:
            item_loader.add_value("images", images)
                    
        utilities = response.xpath("//li/i/span[contains(.,'sur charges')]/following-sibling::span[1]/text()[.!='0']").get()
        if utilities:
            item_loader.add_value("utilities", utilities)
        
        deposit = response.xpath("//li/span[contains(.,'de garantie')]/following-sibling::span[1]/text()[.!='0']").get()
        if deposit:
            item_loader.add_value("deposit", deposit.replace(" ",""))
        lat=response.xpath("//span[@id='latBien']/text()").get()
        if lat:
            item_loader.add_value("latitude",lat)
        lon=response.xpath("//span[@id='lonBien']/text()").get()
        if lon:
            item_loader.add_value("longitude",lon)
            
        energy = response.xpath("//div[contains(@class,'detail-bien-dpe')]/img[contains(@src,'nrj')]/@src").get()
        if energy:
            energy_label = energy.split('-')[-1].split(".")[0].strip()
            if energy_label.isdigit():
                item_loader.add_value("energy_label", energy_label_calculate(energy_label))
       
        item_loader.add_xpath("landlord_name", "//div[@class='detail-bien-contact-form']//div[contains(@class,'contact-agence-agent')][last()]/div[@class='heading3']/text()")
        item_loader.add_xpath("landlord_phone", "//div[@class='detail-bien-contact-form']//div[contains(@class,'contact-agence-agent')][last()]//span[span[.='Tel.']]/text()")
        item_loader.add_xpath("landlord_email", "//div[@class='detail-bien-contact-form']//div[contains(@class,'contact-agence-agent')][last()]//span[span[.='E-mail : ']]/text()")
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