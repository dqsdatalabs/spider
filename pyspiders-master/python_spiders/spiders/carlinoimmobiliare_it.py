# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin
from html.parser import HTMLParser
import re
import dateparser

class MySpider(Spider):
    name = 'carlinoimmobiliare_it'
    external_source = "Carlinoimmobiliare_PySpider_italy"
    execution_type='testing'
    country='italy'
    locale='it' 

    def start_requests(self):

        start_urls = [
            {
                "type" : 10,
                "property_type" : "house"
            },
            {
                "type" : 7,
                "property_type" : "house"
            },
            {
                "type" : 3,
                "property_type" : "apartment"
            },
            {
                "type" : 31,
                "property_type" : "apartment"
            },
            {
                "type" : 47,
                "property_type" : "room"
            },
            

        ] #LEVEL-1

        for url in start_urls:
            r_type = str(url.get("type"))

            formdata = {
                "showkind": "",
                "num_page": "1",
                "group_cod_agenzia": "3941",
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
                "cod_tipologia": str(r_type),
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
            
            yield FormRequest(url="http://www.carlinoimmobiliare.it/web/immobili.asp",
                            callback=self.parse,
                            formdata=formdata,
                            dont_filter=True,
                            meta={'property_type': url.get('property_type'),"type":r_type})
            
    # 1. FOLLOWING
    def parse(self, response):
        r_type = response.meta.get("type")
        page = response.meta.get("page",2)

        for item in response.xpath("//div[@class='row-fluid']/div//a[@class='item-block']/@href").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )
        pagination = response.xpath("//div/a[@rel='next']/@href").get()
        if pagination:
            formdata = {
                "num_page": str(page),
                "showkind": "",
                "group_cod_agenzia": "3941",
                "cod_sede": "0",
                "cod_sede_aw": "0",
                "cod_gruppo": "0",
                "cod_agente": "0",
                "pagref": "0",
                "ref": "",
                "language": "ita",
                "maxann": "10",
                "estero": "0",
                "cod_nazione": "",
                "cod_regione": "",
                "ricerca_testo": "",
                "indirizzo": "",
                "tipo_contratto": "A",
                "cod_categoria": "R",
                "cod_tipologia": str(r_type),
                "cod_provincia": "0",
                "cod_comune": "0",
                "localita": "",
                "prezzo_min": "0",
                "prezzo_max": "100000000",
                "mq_min": "0",
                "mq_max": "10000",
                "vani_min": "0",
                "vani_max": "1000",
                "camere_min": "0",
                "camere_max": "100",
                "riferimento": "",
                "cod_ordine": "O01",
                "garage": "0",
                "ascensore": "0",
                "balcone": "0",
                "soffitta": "0",
                "cantina": "0",
                "taverna": "0",
                "condizionamento": "0",
                "parcheggio": "0",
                "giardino": "0",
                "piscina": "0",
                "camino": "0",
                "prestigio": "0",
                "cod_campi": "",
            }
            
            yield FormRequest(url="http://www.carlinoimmobiliare.it/web/immobili.asp",
                            callback=self.parse,
                            formdata=formdata,
                            dont_filter=True,
                            meta={'property_type': response.meta.get('property_type'),"type":r_type,"page":page+1})
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        item_loader.add_xpath("title", "//h1/text()")
        address = ",".join(response.xpath("//div[@id='det_indirizzo']/span//text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
        else:
            item_loader.add_xpath("address", "//div[@id='det_prov']/span/text()")

        item_loader.add_xpath("external_id", "//div[@id='det_rif']/span/text()")
        item_loader.add_xpath("rent_string", "//span[@class='price colore1 right']//text()")
        item_loader.add_xpath("city", "//div[@id='det_prov']/span/text()")
        item_loader.add_xpath("square_meters", "//li[@id='li_superficie']/strong/text()")
        room_count = response.xpath("//li[@id='li_vani']/strong/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//li[@id='li_camere']/strong/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count)
        item_loader.add_xpath("bathroom_count", "//li[@id='li_bagni']/strong/text()")
        item_loader.add_xpath("floor", "//div[@id='det_piano']/span/text()")
        item_loader.add_xpath("energy_label", "substring-after(//li[@id='li_clen']/text(),': ')")
        item_loader.add_xpath("utilities", "//div[@id='det_spese']/span/text()")
     
        description = " ".join(response.xpath("//div[@class='imm-det-des']//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
      
        elevator = response.xpath("//div[@id='det_ascensore']/span/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)

        terrace = response.xpath("//div[@id='det_terrazza']/span/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
      
        location = response.xpath("//div[@class='map-tab']//iframe/@src").get()
        if location:
            lat = location.split("&sll=")[-1].split(",")[0]
            lng = location.split("&sll=")[-1].split(",")[1].split("&")[0]
            item_loader.add_value("latitude", lat)
            item_loader.add_value("longitude", lng)
        images = [x for x in response.xpath("//ul[@id='photogallery']//li/a//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "CARLINO IMMOBILIARE S.R.L. UNIPERSONALE")
        item_loader.add_value("landlord_phone", "091/6112668")
    
        yield item_loader.load_item()
