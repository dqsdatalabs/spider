# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'francorovaimmobiliare_it'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Francorovaimmobiliare_PySpider_italy"
    

    
    def start_requests(self):
        start_urls = [
            {"url":'https://www.francorovaimmobiliare.it/web/immobili.asp'}
            ] 
        for url in start_urls:
            yield Request(url=url.get('url'),callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen = False
        
        for item in response.xpath("//a[@class='item-block']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        if page == 2 or seen:
            formdata={
                "num_page": f"{page}",
                "showkind": "",
                "group_cod_agenzia": "8198",
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
                "tipo_contratto": "%",
                "cod_categoria": "%",
                "cod_tipologia": "",
                "cod_provincia": "" ,
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
            p_url = f"https://www.francorovaimmobiliare.it/web/immobili.asp"
            yield FormRequest(
                url=p_url,
                formdata=formdata,
                callback=self.parse,
                meta={"property_type" : response.meta.get("property_type"), "page":page+1},
            )
            

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        dontallow=response.xpath("//strong[.='Vendita']").get()
        if dontallow:
            return 
        dontallow2=response.xpath("//li[@id='li_tipologia']/strong[.='Ufficio']").get()
        if dontallow2:
            return 
        external_id = response.xpath(
            "//div[@class='sfondo_colore3 colore1 right padder']//following-sibling::strong//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)

        title = response.xpath(
            "//title//text()").get()
        if title:
            item_loader.add_value("title", title)

        address = response.xpath(
            "//div[@class='anprovloc']//text()").getall()
        if address:
            item_loader.add_value("address", address)

        city = response.xpath(
            "//h3[@class='no-btm ancom']//text()").get()
        if city:
            item_loader.add_value("city", city)

        description = response.xpath(
            "//div[@class='imm-det-des']//text()").getall()
        if description:
            item_loader.add_value("description", description)

        square_meters = response.xpath(
            "//ul[@class='feature-list']//li[@id='li_superficie']//strong//text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters)

        bathroom_count = response.xpath(
            "//ul[@class='feature-list']//li[@id='li_bagni']//strong//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        room_count = response.xpath(
            "//ul[@class='feature-list']//li[@id='li_vani']//strong//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)

        energy_label = response.xpath(
            "//ul[@class='feature-list']//li[@id='li_clen']//strong//following-sibling::text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split(":"))

        rent = response.xpath(
            "//div[@class='span3 single-property']//span[@class='price colore1 right']//text()").get()
        if rent:
            item_loader.add_value("rent", rent.split("€"))
        item_loader.add_value("currency", "EUR")

        images = [response.urljoin(x) for x in response.xpath(
            "//div[@class='photoslide']//div[contains(@class,'watermark')]//img//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        floor_plan_images = [response.urljoin(x) for x in response.xpath(
            "//div[@class='photoslide']//div[contains(@class,'watermark')]//img[contains(@alt,'Planimetria')]//@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        item_loader.add_value("landlord_name", "Franco Rovaı")
        item_loader.add_value("landlord_phone", "338 2494306")


        yield item_loader.load_item()