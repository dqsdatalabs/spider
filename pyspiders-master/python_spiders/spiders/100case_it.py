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
    name = '100case_it'
    execution_type='testing'
    country='italy'
    locale='it' # LEVEL 1
    scale_separator ='.'
    external_source = "100Case_PySpider_italy"

    def start_requests(self):
        start_urls = [
            {
                "cod_tipologia" : 3,
                "property_type" : "apartment"
            },
            {
                "cod_tipologia" : 10,
                "property_type" : "house"
            },
            

        ] #LEVEL-1
        seen = False
        for url in start_urls:
            cod_tipologia = str(url.get("cod_tipologia"))

            data = {
                "showkind": "",
                "num_page": "1",
                "group_cod_agenzia": "7969",
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
                "tipo_contratto": "%",
                "cod_categoria": "R",
                "cod_tipologia": cod_tipologia,
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

            yield FormRequest(
                "https://www.100case.it/web/immobili.asp",
                formdata=data,
                callback=self.parse,
                meta = {"property_type":url.get("property_type"),"cod_tipologia":cod_tipologia}
            )


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        cod_tipologia = response.meta.get("cod_tipologia")
        seen = False
        for item in response.xpath("//div[@class='property-item']"):
            f_url = response.urljoin(item.xpath(".//h4/a/@href").extract_first())
            # room_count = item.xpath(".//div[@class='features']/div[@class='group accessories']/span[@class='bed icon']/text()").extract_first()
            bathroom_count = item.xpath(".//div[@class='features']/div[@class='group accessories']/span[@class='bath icon']/text()").extract_first()
            s_meters = "".join(item.xpath(".//div[@class='features']/div[@class='group accessories']/span[@class='area icon']/strong/text()").extract())
            yield Request(
                f_url, 
                callback=self.populate_item, meta={"bathroom_count":bathroom_count,"s_meters":s_meters,"property_type":response.meta.get("property_type")}
            )
            seen=True

        if page == 2 or seen:
            print(page)
            
            data = {
                "showkind": "",
                "num_page": f"{page}",
                "group_cod_agenzia": "7969",
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
                "tipo_contratto": "%",
                "cod_categoria": "R",
                "cod_tipologia": f"{cod_tipologia}",
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

            yield FormRequest(
                "https://www.100case.it/web/immobili.asp",
                formdata=data,
                callback=self.parse,
                meta = {"page":page+1,"property_type":response.meta.get("property_type")}
            )
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_xpath("title", "//title/text()")
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get("property_type"))

        keywords = response.xpath("//meta[@name='keywords']/@content").get()
        if keywords:
            if "vendita" in keywords:
                return

        item_loader.add_value("address", response.meta.get("address"))
        item_loader.add_value("bathroom_count", response.meta.get("bathroom_count"))
        item_loader.add_value("square_meters", response.meta.get("s_meters"))
        item_loader.add_xpath("external_id", "//div[@id='det_rif']/@data-valore")
        item_loader.add_xpath("energy_label", "//div[@id='det_cl_en']/@data-valore[.!='Non indicata']")
        item_loader.add_xpath("utilities", "//div[@id='det_spese']/@data-valore")
        item_loader.add_xpath("floor", "//div[@id='det_piano']/@data-valore")

        room = response.xpath("//div[@id='det_vani']/@data-valore").get()
        if room:
            item_loader.add_value("room_count",room)

        city = response.xpath("//div[@id='det_prov']/@data-valore").get()
        if city:
            item_loader.add_value("city",city)

        address = response.xpath("//div[@id='det_zona']/@data-valore").get()
        if address:
            item_loader.add_value("address",  city + address)

        position = response.xpath("//iframe[@id='gmap']/@src").get()
        if position:
            lat = re.search("=([\d.]+),",position).group(1)
            long = re.search(",([\d.]+)&",position).group(1)
            item_loader.add_value("latitude",lat)
            item_loader.add_value("longitude",long)




        rent = "".join(response.xpath("//span[@class='price colore3']/text()").extract())
        if rent:
            item_loader.add_value("rent_string", rent.replace(".","").strip())

        desc="".join(response.xpath("//div[@id='dex_annuncio']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        furnished =" ".join(response.xpath("//div[@id='det_arredato']/@data-valore").getall())
        if furnished:
            item_loader.add_value("furnished", True)

        elevator =" ".join(response.xpath("//div[@id='det_ascensore']/@data-valore").getall())
        if elevator:
            item_loader.add_value("elevator", True)
            
        
        images=[x for x in response.xpath("//ul[@class='photogallery hiddenmobile']/li/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name","LA CENTOCASE IMMOBILIARE SAS")
        item_loader.add_value("landlord_email","info@100case.it")
        item_loader.add_value("landlord_phone","055268644")

        yield item_loader.load_item()