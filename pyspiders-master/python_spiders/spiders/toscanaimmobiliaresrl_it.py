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
    name = 'toscanaimmobiliaresrl_it'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Toscanaimmobiliaresrl_PySpider_italy"
    start_urls = ['https://www.toscanaimmobiliaresrl.it/elenco_immobili_f.asp?rel=nofollow']  # LEVEL 1

    formdata = {
        "riferimento": "", 
        "cod_istat": "",
        "idcau": "2",
        "idtip": "5",
        "a_prezzo": "",
        "da_mq": "",
        "nvani": "0",
        "nr_camereg": "0",
        "nr_servizi": "0",
    }

    def start_requests(self):
        start_urls = [
            {
                "type": [
                    "5", "57",
                ],
                "property_type": "apartment"
            },
	        {
                "type": [
                    "55", "16", "36", "56", "68", "37", "58", "14", "13", "52", "7", "63", "64"
                ],
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('type'):
                self.formdata["idtip"] = item
                yield FormRequest(
                    url=self.start_urls[0],
                    dont_filter=True,
                    formdata=self.formdata,
                    callback=self.parse,
                    meta={
                        'property_type': url.get('property_type'),
                        "type": item
                    }
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[contains(.,'Vedi Dettagli')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title","//title//text()")
        external_id=item_loader.get_output_value("title")
        if external_id:
            item_loader.add_value("external_id",external_id.split("rif:")[-1].strip())
        rent=response.xpath("//strong[.='Prezzo:']/following-sibling::text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[-1].replace(".",""))
        square_meters=response.xpath("//strong[.='Superficie:']/following-sibling::text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(".")[-1])
        room_count=response.xpath("//strong[.='Camere:']/following-sibling::text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.strip())
        desc=" ".join(response.xpath("//div[@class='detail-title']/following-sibling::p/text()").getall())
        if desc:
            item_loader.add_value("description",desc)
        images=[x for x in response.xpath("//section[@class='section-detail-content']//img//@src").getall()]
        if images:
            item_loader.add_value("images",images)
        energy_label=response.xpath("//strong[.='Classe Energetica: ']/following-sibling::text()").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label)
        terrace=response.xpath("//ul[@class='list-three-col list-features']//li//a[contains(.,'Terrazzo')]").get()
        if terrace:
            item_loader.add_value("terrace",True)
        balcony=response.xpath("//ul[@class='list-three-col list-features']//li//a[contains(.,'Balcone')]").get()
        if balcony:
            item_loader.add_value("balcony",True)
        utilities=response.xpath("//ul[@class='list-three-col list-features']//li//a[contains(.,'Spese cond. mensili:')]").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split(":")[-1].replace("€",""))
        item_loader.add_value("landlord_name","Toscana immobiliare sr")
        item_loader.add_value("landlord_email","amministrazione@toscanaimmobiliaresrl.it")
        item_loader.add_value("landlord_phone","055/3890471")

        item_loader.add_value("currency","EUR")
        address = response.xpath("//div[@class='col-sm-12']/h1/text()").get()
        if address:
            address = address.split(" a ")[-1]
            item_loader.add_value("address",address)

        city = response.xpath("//strong[text()='Comune:']/following-sibling::text()").get()
        if city:
            item_loader.add_value("city",city)

        parking = response.xpath("//a[contains(text(),'Box Auto:')]/text()[not(contains(.,'nessuno'))]").get()
        if parking:
            item_loader.add_value("parking",True)

        

        yield item_loader.load_item()