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

class MySpider(Spider):
    name = 'centroimmobiliaretoscana_it'
    execution_type='testing'
    country='italy'
    locale='it' 
    external_source = "Centroimmobiliaretoscana_PySpider_italy"
    start_urls = ['http://www.centroimmobiliaretoscana.it/web/immobili.asp']  # LEVEL 1
    
    formdata = {
        "showkind": "",
        "num_page": "1",
        "group_cod_agenzia": "4809",
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
        "cod_tipologia": "3",
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
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36'
    }
    
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "3", "30","24"
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "31", "34", "7", "22", "12", "24", "10",  "32", "33", "29"
                ],
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                self.formdata["cod_tipologia"] = item
                yield FormRequest(
                    url=self.start_urls[0],
                    dont_filter=True,
                    formdata=self.formdata,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type'), "type": item}
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//a[@class='item-block']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            self.formdata["cod_tipologia"] = response.meta.get('type')
            self.formdata["num_page"] = str(page)
            yield FormRequest(
                self.start_urls[0], 
                dont_filter=True, 
                callback=self.parse, 
                formdata=self.formdata, 
                headers=self.headers, 
                meta={"page": page+1, "property_type": response.meta.get('property_type'), "type": response.meta.get('type')}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        external_id = response.xpath("//title//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split("rif. ")[1])

        title = response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title", title)

        description = response.xpath("//div[contains(@class,'imm-det-des')]//text()").getall()
        if description:
            item_loader.add_value("description", description)

        square_meters = response.xpath("//ul[contains(@class,'feature-list')]//li[contains(@id,'li_superficie')]//strong//text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters)

        rent = response.xpath("//div[contains(@class,'span3 single-property')]//span[contains(@class,'price colore1 right')]//text()").get()
        if rent:
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        bathroom_count = response.xpath("//ul[contains(@class,'feature-list')]//li[contains(@id,'li_bagni')]//strong//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        room_count = response.xpath("//ul[contains(@class,'feature-list')]//li[contains(@id,'li_vani')]//strong//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)

        energy_label = response.xpath("//ul[contains(@class,'feature-list')]//li[contains(@id,'li_clen')]//strong//following-sibling::text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split(": "))

        address = response.xpath("//div[contains(@id,'det_zona')]//strong//following-sibling::span//text()").get()
        if address:
            item_loader.add_value("address", address.split("- "))

        city = response.xpath("//div[contains(@id,'det_prov')]//strong//following-sibling::span//text()").get()
        if city:
            item_loader.add_value("city", city)

        floor_plan_images = [response.urljoin(x) for x in response.xpath("//div[contains(@id,'box_planimetrie')]//ul[contains(@id,'plangallery')]//li[contains(@class,'span3')]//a//@data-img").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        images = [response.urljoin(x) for x in response.xpath("//div[contains(@id,'box_fotografie')]//ul[contains(@id,'photogallery')]//li[contains(@class,'span3')]//a//@data-img").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "Centro Immobiliare della Toscana")
        item_loader.add_value("landlord_phone", "0558495084")
        item_loader.add_value("landlord_email", "info@centroimmobiliaretoscana.it")

        yield item_loader.load_item()