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
    name = 'artediabitare_it'
    external_source = "Artediabitare_PySpider_italy"
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
                "type" : 34,
                "property_type" : "house"
            },
            {
                "type" : 3,
                "property_type" : "apartment"
            },
            {
                "type" : 30,
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
                "num_page": "1",
                "group_cod_agenzia": "7655",
                "cod_sede": "0",
                "cod_sede_aw": "0",
                "cod_gruppo": "61",
                "pagref": "0",
                "ref": "",
                "language": "ita",
                "cod_nazione": "",
                "cod_regione": "",
                "cod_provincia": "",
                "cod_comune": "0",
                "indirizzo": "",
                "tipo_contratto": "A",
                "cod_tipologia": str(r_type),
                "camere_min": "0",
                "riferimento": "",
                "prezzo_max":  "",
                "vani_min": "0",
                "giardino": "0",
                "cod_ordine": "O03"
            }
            
            yield FormRequest(url="https://www.artediabitare.it/web/immobili.asp",
                            callback=self.parse,
                            formdata=formdata,
                            dont_filter=True,
                            meta={'property_type': url.get('property_type'),"type":r_type})
            
    # 1. FOLLOWING
    def parse(self, response):
        r_type = response.meta.get("type")
        page = response.meta.get("page",2)

        for item in response.xpath("//div[@class='listing']/div//div[@class='pulsanti']/a/@href").extract():
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
                "group_cod_agenzia": "7655",
                "cod_sede": "0",
                "cod_sede_aw": "0",
                "cod_gruppo": "61",
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
                "cod_ordine": "O03",
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
            
            yield FormRequest(url="https://www.artediabitare.it/web/immobili.asp",
                            callback=self.parse,
                            formdata=formdata,
                            dont_filter=True,
                            meta={'property_type': response.meta.get('property_type'),"type":r_type,"page":page+1})
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        address = response.xpath("//div[@id='det_indirizzo']//span/text()").get()
        if address:
            item_loader.add_value("address", address)
        else:
            item_loader.add_xpath("address", "//div[@id='det_prov']//span/text()")

        item_loader.add_xpath("title", "//div[@class='box titolo']/h2/text()")
        item_loader.add_xpath("external_id", "//div[@id='det_rif']//span/text()")
        item_loader.add_xpath("rent_string", "//div[@id='det_prezzo']//span/text()")
        item_loader.add_xpath("city", "//div[@id='det_prov']//span/text()")
        item_loader.add_xpath("square_meters", "//div[@id='det_superficie']/@data-valore")
        room_count = response.xpath("//div[@class='nrvani']/span[@class='valore']/text()").get()
        if room_count:
            if "," in room_count:
                room_count = room_count.split(",")[0]
            item_loader.add_value("room_count",room_count)
        # item_loader.add_xpath("room_count", "//div[@class='nrvani']/span[@class='valore']/text()")
        item_loader.add_xpath("bathroom_count", "//div[@id='det_bagni']//span/text()")
        item_loader.add_xpath("floor", "substring-before(//div[@id='det_piano']//span/text(),' /')")
        item_loader.add_xpath("energy_label", "//div[@id='det_cl_en']/@data-valore")
        item_loader.add_xpath("utilities", "//div[@id='det_spese']//span/text()")
     
        description = " ".join(response.xpath("//div[h2[.='Descrizione']]//text()[.!='Descrizione']").getall())
        if description:
            item_loader.add_value("description", description.strip())
      
        elevator = response.xpath("//div[@id='det_ascensore']//span/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)

        balcony = response.xpath("//div[@id='det_balcone']//span/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        parking = response.xpath("//div[@id='det_garage']//span/text()").get()
        if parking:
            item_loader.add_value("parking", True)
            
        furnished = response.xpath("//div[@id='det_arredato']//span/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        location = response.xpath("//iframe[@class='gmap']/@src").get()
        if location:
            lat = location.split("&sll=")[-1].split(",")[0]
            lng = location.split("&sll=")[-1].split(",")[1].split("&")[0]
            item_loader.add_value("latitude", lat)
            item_loader.add_value("longitude", lng)
        images = [x for x in response.xpath("//div[@class='swiper-container']//div[@class='swiper-slide']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_xpath("landlord_name", "//h2[@class='agency_name']//text()")
        landlord_phone = response.xpath("//span[@class='agency_recapiti callaction']/a//text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.strip())

        landlord_email = response.xpath("//div[@class='agency_email']/a/text()").get()
        if landlord_email:
            item_loader.add_value("landlord_email",landlord_email)

        yield item_loader.load_item()
