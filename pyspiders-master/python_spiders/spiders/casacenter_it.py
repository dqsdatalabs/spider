# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.http import headers
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider):
    name = 'casacenter_it'
    external_source = "Casacenter_PySpider_italy"
    execution_type='testing'
    country='italy'
    locale='it' 
    start_urls = ['https://www.casacenter.it/web/immobili.asp']  # LEVEL 1

    formdata = {
        "showkind": "",
        "num_page": "1",
        "group_cod_agenzia": "8179",
        "cod_sede": "0",
        "cod_sede_aw": "0",
        "cod_gruppo": "0",
        "pagref": "",
        "ref": "",
        "language": "ita",
        "maxann": "10",
        "estero": "0",
        "cod_nazione": "",
        "cod_regione": "",
        "tipo_contratto": "A",
        "cod_categoria": "R",
        "cod_tipologia": "0",
        "cod_provincia": "0",
        "cod_comune": "0",
        "prezzo_min": "",
        "prezzo_max": "",
        "mq_min": "",
        "mq_max": "",
        "vani_min": "",
        "vani_max": "",
        "camere_min": "",
        "camere_max": "",
        "riferimento": "",
        "cod_ordine": "O01",
    }
    
    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36"
    }
    
    def start_requests(self):
        yield FormRequest(self.start_urls[0], callback=self.parse, formdata=self.formdata, headers=self.headers)
    
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//article[@class='annuncio']"):
            follow_url = response.urljoin(item.xpath(".//a[@class='item-block']/@href").get())
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            self.formdata["num_page"] = str(page)
            yield FormRequest(
                self.start_urls[0], 
                dont_filter=True, 
                callback=self.parse, 
                formdata=self.formdata, 
                headers=self.headers, 
                meta={
                    "page": page+1,
                }
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        
        item_loader.add_xpath("title", "//title/text()")
        
        property_type = response.xpath("//title/text()").get()
        desc = "".join(response.xpath("//div[@class='imm-det-des']//text()").getall())
        if get_p_type_string(property_type):
            item_loader.add_value("property_type", get_p_type_string(property_type))
        else:
            if get_p_type_string(desc):
                item_loader.add_value("property_type", get_p_type_string(desc))
            else:
                return
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        item_loader.add_xpath("rent", "//meta[contains(@property,'price:amount')]/@content")
        item_loader.add_value("currency", "EUR")
        
        item_loader.add_xpath("square_meters", "//li[@id='li_superficie']/strong/text()")
        item_loader.add_xpath("room_count", "//li[@id='li_vani']/strong/text()")
        item_loader.add_xpath("bathroom_count", "//li[@id='li_bagni']/strong/text()")

        energy_label = response.xpath("//li[@id='li_clen']/text()").get()
        if energy_label:
            energy_label = energy_label.split(":")[1].strip()
            item_loader.add_value("energy_label", energy_label)
        
        address = "".join(response.xpath("//div[@class='anprovloc']//text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
        
        item_loader.add_xpath("city", "//div[@id='det_prov']//@data-valore")
        
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc.strip()))
        
        images = [x for x in response.xpath("//ul[@class='photogallery']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_xpath("external_id", "//div[@id='det_rif']//@data-valore")
        
        floor = response.xpath("//div[@id='det_piano']/span/text()").get()
        if floor:
            if "/" in floor: floor = floor.split("/")[0].strip()
            item_loader.add_value("floor", floor)
        
        furnished = response.xpath("//div[@id='det_arredato']/span/text()").get()
        if furnished and "si" in furnished.lower():
            item_loader.add_value("furnished", True)

        balcony = response.xpath("//div[@id='det_balcone']/span/text()").get()
        if balcony and "si" in balcony.lower():
            item_loader.add_value("balcony", True)

        elevator = response.xpath("//div[@id='det_ascensore']/span/text()").get()
        if elevator and "si" in elevator.lower():
            item_loader.add_value("elevator", True)
        
        terrace = response.xpath("//div[@id='det_terrazza']/span/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        utilities = response.xpath("//div[@id='det_spese']/span/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[1].strip())
        
        latitude_longitude = response.xpath("//iframe/@src[contains(.,'map')]").get()
        if latitude_longitude:
            lat = latitude_longitude.split("q=")[1].split(",")[0]
            lng = latitude_longitude.split("q=")[1].split(",")[1].split("&")[0]
            item_loader.add_value("latitude", lat)
            item_loader.add_value("longitude", lng)

        item_loader.add_value("landlord_name", "CASA CENTER")
        item_loader.add_value("landlord_phone", "0957221361")
        item_loader.add_value("landlord_email", "CASACENTER@TISCALI.IT")

        reserved_ads = response.xpath("//span[text()='Tratt. riservata']").get()
        if reserved_ads:
            return

        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("appartamento" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("casa" in p_type_string.lower() or "villetta" in p_type_string.lower() or "villa" in p_type_string.lower() or "attico" in p_type_string.lower() or "villino" in p_type_string.lower() or "mansarda" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and "stanza" in p_type_string.lower():
        return "room"
    else:
        return None