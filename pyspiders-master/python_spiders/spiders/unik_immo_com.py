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
    name = 'unik_immo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        yield Request("http://www.unik-immo.com/fr/liste.htm?menu=3&page=1", callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'liste-bien-container')]"):
            follow_url = response.urljoin(item.xpath(".//a[contains(.,'Plus de détails')]/@href").get())
            property_type = item.xpath(".//div[@class='liste-bien-type']/text()").get()
            if get_p_type_string(property_type): yield Request(follow_url, callback=self.populate_item, meta={"property_type": get_p_type_string(property_type)})
        
        next_button = response.xpath("//span[@class='PageSui']/a/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse)
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", "Unik_Immo_PySpider_france")

        external_id = response.xpath("//li[span[.='Ref']][last()]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        city = response.xpath("//li[span[.='Ville']][last()]/text()").get()
        if city:
            item_loader.add_value("title", city)
            item_loader.add_value("city", city.strip())
            item_loader.add_value("address", city)
        
        rent = " ".join(response.xpath("//li[@class='prix']/text()").getall())
        if rent:
            item_loader.add_value("rent", rent.split(":")[-1].replace(" ","").split(".")[0])
            item_loader.add_value("currency", "EUR")

        description = " ".join(response.xpath("//div[@class='detail-bien-desc-content']/p//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())

        room_count = response.xpath("//li[span[.='Chambres']]/text()[.!=' NC']").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            item_loader.add_xpath("room_count", "//li[span[.='Pièces']]/text()[.!=' NC']")

        square_meters = response.xpath("//li[span[.='Surface']]/text()[.!=' NC']").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].strip())
        deposit = response.xpath("//li//span[contains(.,'Dépôt de garantie')]/following-sibling::span[1]/text()").get()
        if deposit:
            item_loader.add_value("deposit", int(float(deposit.replace(" ","").strip())))
        item_loader.add_xpath("utilities", "//li//span[contains(.,'provisions sur charges')]/following-sibling::span[1]/text()[.!='0']")

        energy_label = response.xpath("//img[@class='img-nrj img-dpe']/@src").get()
        if energy_label:
            energy_label = energy_label.split("nrj-w-")[-1].split("-")[0].strip()
            if energy_label in ["A","B","C","D","E","F","G"]:
                item_loader.add_value("energy_label", energy_label)

        available_date = response.xpath("//div[@class='detail-bien-desc-content']/p//text()[contains(.,'Libre le ')]").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split("Libre le ")[-1].strip(), date_formats=["%d %B %Y"], languages=['fr'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        else:
            available_date = response.xpath("//p//text()[contains(.,'Dispo') and contains(.,' le ')]").get()
            if available_date:
                available_date = available_date.split(" le ")[1].strip().replace(":","-").replace("!","-")
                date_parsed = dateparser.parse(available_date.split("Libre le ")[-1].strip(), date_formats=["%d %B %Y"], languages=['fr'])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
            
        item_loader.add_xpath("latitude", "//li[@class='gg-map-marker-lat']/text()")
        item_loader.add_xpath("longitude", "//li[@class='gg-map-marker-lng']/text()")
     
        images = [response.urljoin(x) for x in response.xpath("//div[@class='big-flap-container']/div[@class='diapo is-flap']//img/@data-src").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "UNIK IMMOBILIER")
        item_loader.add_value("landlord_phone", "04.34.22.10.12")

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "local" in p_type_string.lower():
        return None
    elif p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("studio" in p_type_string.lower() or "t1" in p_type_string.lower()):
        return "studio"
    elif p_type_string and ("appartement" in p_type_string.lower() or "f1" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "t2" in p_type_string.lower() or "t3" in p_type_string.lower() or "t4" in p_type_string.lower() or "t5" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("maison" in p_type_string.lower() or "villa" in p_type_string.lower()):
        return "house"
    elif p_type_string and "chambre" in p_type_string.lower():
        return "room"   
    else:
        return None