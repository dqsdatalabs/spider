# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'mammutimmobiliare_it'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Mammutimmobiliare_PySpider_italy"
    start_urls = ['http://www.mammutimmobiliare.it/web/immobili.asp']  # LEVEL 1

    formdata = {
        "showkind": "",
        "num_page": "1",
        "group_cod_agenzia": "8041",
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
        "cod_ordine": "O01"
    }
    
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "3","30"
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "31", "34", "12", "24", "10"
                ],
                "property_type": "house"
            },
            {
                "url": [
                    "47"
                ],
                "property_type": "room"
            }
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
        for item in response.xpath("//a[contains(.,'Continua')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        
        if page==2 or seen:
            self.formdata["num_page"] = f"{page}"
            self.formdata["cod_tipologia"] = response.meta.get('type')
            
            yield FormRequest(
                self.start_urls[0],
                dont_filter=True,
                formdata=self.formdata,
                callback=self.parse,
                meta={
                    "property_type": response.meta.get('property_type'),
                    "type": response.meta.get('type'),
                    "page": page+1
                }
            )
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        external_id = response.xpath(
            "//div[contains(@id,'det_rif')]//span[@class='valore']//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)

        title = response.xpath(
            "//title//text()").get()
        if title:
            item_loader.add_value("title", title)

        city = response.xpath("//div[@id='det_prov']/span[@class='valore']/text()").get()
        if city:
            item_loader.add_value("city", city)
        
        description = response.xpath(
            "//div[@class='grid_8']//div[contains(.,'Descrizione')]//following-sibling::text()").getall()
        if description:
            item_loader.add_value("description", description)

        rent = response.xpath(
            "//div[contains(@id,'det_prezzo')]//span[@class='valore']//text()").get()
        if rent:
            item_loader.add_value("rent", rent.split("€"))
        item_loader.add_value("currency", "EUR")

        square_meters = response.xpath(
            "//div[contains(@id,'det_superficie')]//span[@class='valore']//text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("mq"))

        bathroom_count = response.xpath(
            "//div[contains(@id,'det_bagni')]//span[@class='valore']//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        room_count = response.xpath(
            "//div[contains(@id,'det_vani')]//span[@class='valore']//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(',')[0])

        energy_label = response.xpath(
            "//div[contains(@id,'det_cl_en')]//span[@class='valore']//text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)

        utilities = response.xpath(
            "//div[contains(@id,'det_spese')]//span[@class='valore']//text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split("€"))

        furnished = response.xpath(
            "//div[contains(@id,'det_arredato')]//span[@class='valore']/text()[contains(.,'si')]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        images = [response.urljoin(x) for x in response.xpath(
            "//div[@class='slider_det']//ul[@class='items']//li//a[@class='gal']//img//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "Mammut Immobiliare sas di Lucia Incatasciato & c.")
        item_loader.add_value("landlord_phone", "055366853")


        map_iframe = response.xpath("//iframe[contains(@src,'https://maps.google.it/maps?')]/@src").get()
        if map_iframe: yield Request(map_iframe, callback=self.get_map, dont_filter=True, meta={"item_loader": item_loader})

    def get_map(self, response):
        item_loader = response.meta["item_loader"]
        latitude = response.xpath("//div[@id='mapDiv']/following-sibling::script[1]/text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split('",null,[null,null,')[1].split(',')[0].strip())
            item_loader.add_value("longitude", latitude.split('",null,[null,null,')[1].split(',')[1].split(']')[0].strip())

        address = "".join(response.xpath("//script/text()[contains(.,'onEmbedLoad')]").extract())
        if address:
            address = address.split(',null,null,null,null,null,null,null,null,null,"')[-1].split('",null,null,null,null,')[0].strip()
            if address and "via" in address.lower():
                item_loader.add_value("address", address.split('Via')[-1].strip())
            else:
                item_loader.add_value("address", address)
        
            zip = address.split('Via')[-1].strip()
            if zip and "," in zip:
                if zip.count(',') == 2:
                    zipcode = zip.split(', ')[1].split(' ')[0].strip()
                    item_loader.add_value("zipcode", zipcode)
                elif zip.count(',') == 3:
                    zipcode = zip.split(', ')[2].split(' ')[0].strip()
                    item_loader.add_value("zipcode", zipcode)
                else:
                    zipcode = zip.split(' ')[0].strip()
                    item_loader.add_value("zipcode", zipcode)

        
        
        yield item_loader.load_item()