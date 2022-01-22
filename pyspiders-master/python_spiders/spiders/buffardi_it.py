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
    name = 'buffardi_it' 
    execution_type='testing' 
    country='italy'
    locale='it' 
    external_source = "Buffardi_PySpider_italy"
    def start_requests(self):
 
        start_urls = [
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
            
            yield FormRequest(url="http://www.buffardi.it/web/immobili.asp",
                            callback=self.parse,
                            formdata=formdata,
                            dont_filter=True,
                            meta={'property_type': url.get('property_type'),"type":r_type})


    def parse(self, response):
        r_type = response.meta.get("type")
        page = response.meta.get("page",2)

        for item in response.xpath("//article//a//@href").extract():
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
            
            yield FormRequest(url="http://www.buffardi.it/web/immobili.asp",
                            callback=self.parse,
                            formdata=formdata,
                            dont_filter=True,
                            meta={'property_type': response.meta.get('property_type'),"type":r_type,"page":page+1})
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title","//title/text()")
        external_id=response.url
        if external_id:
            item_loader.add_value("external_id",external_id.split("annuncio=")[-1].split("&")[0])

        rent=response.xpath("//h3[@class='price']/text()").get()
        if rent:
            rent=rent.split("€")[-1].strip().replace(".","")
            rent=re.findall("\d+",rent)
            if rent:
                item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")
        adres=response.xpath("//strong[contains(.,'Provincia')]/following-sibling::span/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        city=response.xpath("//strong[contains(.,'Zona')]/following-sibling::span/text()").get()
        if city:
            item_loader.add_value("city",city)
        room_count=response.xpath("//strong[contains(.,'Camere')]/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//strong[contains(.,'Bagni')]/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        square_meters=response.xpath("//strong[contains(.,'Superficie')]/following-sibling::span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(" ")[0])
        energy_label=response.xpath("//strong[contains(.,'Classe energetica')]/following-sibling::span/text()").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label)
        furnished=response.xpath("//strong[contains(.,'Arredato')]/following-sibling::span/text()").get()
        if furnished:
            item_loader.add_value("furnished",True)
        elevator=response.xpath("//strong[contains(.,'Ascensore')]/following-sibling::span/text()").get()
        if elevator:
            item_loader.add_value("elevator",True)
        floor=response.xpath("//strong[contains(.,'Piano')]/following-sibling::span/text()").get()
        if floor:
            item_loader.add_value("floor",floor)
        desc=response.xpath("//div[@itemprop='description']/text()").get()
        if desc:
            item_loader.add_value("description",desc)
        images=[x for x in response.xpath("//section//article//div[@class='row photogallery']/div/figure//img//@src").getall()]
        if images:
            item_loader.add_value("images",images)
        phone=response.xpath("//span[@itemprop='telephone']/text()").get()
        if phone:
            item_loader.add_value("landlord_phone",phone)
        name=response.xpath("//strong[@itemprop='name']/text()").get()
        if name:
            item_loader.add_value("landlord_name",name)
        latitude=response.xpath("//iframe//@src").get()
        if latitude:
            item_loader.add_value("latitude",latitude.split("https://maps.google.it/maps?f=q&q=")[-1].split("&sll=")[0].split(",")[0])
        longitude=response.xpath("//iframe//@src").get()
        if longitude:
            item_loader.add_value("longitude",longitude.split("https://maps.google.it/maps?f=q&q=")[-1].split("&sll=")[0].split(",")[-1])
        
        
        
        yield item_loader.load_item()