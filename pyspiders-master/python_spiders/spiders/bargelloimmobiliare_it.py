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
    name = 'bargelloimmobiliare_it'
    external_source = "Bargelloimmobiliare_it_PySpider_italy"
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
                "showkind":"",
                "num_page": "1",
                "group_cod_agenzia": "6650",
                "cod_sede": "0",
                "cod_sede_aw": "0",
                "cod_gruppo": "0",
                "pagref": "",
                "ref": "",
                "language": "ita",
                "cod_nazione": "",
                "cod_regione": "",
                "cod_provincia": "",
                "cod_comune": "0",
                "indirizzo": "",
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
                "cod_ordine": "O01"
            }
            
            yield FormRequest(url="http://www.bargelloimmobiliare.it/web/immobili.asp",
                            callback=self.parse,
                            formdata=formdata,
                            dont_filter=True,
                            meta={'property_type': url.get('property_type'),"type":r_type})
            
    # 1. FOLLOWING
    def parse(self, response):
        r_type = response.meta.get("type")
        page = response.meta.get("page",2)

        for item in response.xpath("//div[@class='listing']/div//div[@class='clipimg']/a/@href").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )
        pagination = response.xpath("//div/a[@class='pulsante']/@href").get()
        if pagination:
            formdata = {
                "num_page": str(page),
                "showkind":"",
                "group_cod_agenzia": "6650",
                "cod_sede": "0",
                "cod_sede_aw": "0",
                "cod_gruppo": "0",
                "pagref": "",
                "ref": "",
                "language": "ita",
                "cod_nazione": "",
                "cod_regione": "",
                "cod_provincia": "",
                "cod_comune": "0",
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
            
            yield FormRequest(url="http://www.bargelloimmobiliare.it/web/immobili.asp",
                            callback=self.parse,
                            formdata=formdata,
                            dont_filter=True,
                            meta={'property_type': response.meta.get('property_type'),"type":r_type,"page":page+1})
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title","//title//text()")
        external_id=response.xpath("//div[@id='det_rif']/span/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
        rent=response.xpath("//div[@class='prezzo']/text()").get()
        if rent:
            rent=rent.replace(".","").replace("€","").strip()
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")
        room_count=response.xpath("//div[@id='det_vani']/span/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//div[@id='det_bagni']/span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        square_meters=response.xpath("//div[@id='det_superficie']/span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters)
        energy_label=response.xpath("//div[@id='det_cl_en']/span/text()").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label.split(":")[-1].split("(")[0])
        utilities=response.xpath("//div[@id='det_spese']/span/text()").get()
        if utilities:
            utilities=utilities.replace(".","").replace("€","").strip()
            item_loader.add_value("utilities",utilities)
        description=" ".join(response.xpath("//div[.='Descrizione']/following-sibling::text()").getall())
        if description:
            item_loader.add_value("description",description)
        images=[x for x in response.xpath("//div[@class='sliderkit-nav-clip']/ul//li/a/img/@src").getall()]
        if images:
            item_loader.add_value("images",images) 
        item_loader.add_value("landlord_name","Bargello Immobiliare")
        item_loader.add_value("landlord_email","info@bargelloimmobiliare.it")
        item_loader.add_value("landlord_phone","055 245797")

        city=response.xpath("//div[@id='det_prov']/span/text()").get()
        if city:
            item_loader.add_value("city",city)

        address=response.xpath("//div[@id='det_zona']/span/text()").get()
        if address:
            item_loader.add_value("address",address)        

        furnished=response.xpath("//div[@id='det_arredato']").get()
        if furnished:
            item_loader.add_value("furnished",True)         

        yield item_loader.load_item()