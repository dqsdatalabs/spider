# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.http import headers
from scrapy.loader.processors import MapCompose
from scrapy import Spider, item
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re
 
class MySpider(Spider):
    name = 'cscase_it'
    execution_type='testing'
    country='italy'
    locale='it' 
    external_source = "Cscase_PySpider_italy"
    post_urls = ['http://www.cscase.it/immobili.php?op=cerca']  # LEVEL 1

    formdata = {
        "inaffitto":"" ,
        "categoria": "1",
        "id_tipo": "1",
        "citta": "",
        "zona": "0",
        "id_sottozona": "0",
        "s_prezzo": "=",
        "prezzo": "",
        "s_vani": "=",
        "vani": "",
        "arredato": "",
    }
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36"
    }
    
    def start_requests(self):
        
        start_urls = [
            {
                "type": [
                    "1",
                ],
                "property_type": "apartment"
            },
	        {
                "type": [
                    "2","3","4","5","6","7","21","22"
                ],
                "property_type": "house"
            },
            {
                "type": [
                    "13",
                ],
                "property_type": "studio"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('type'):
                self.formdata["id_tipo"] = item
                yield FormRequest(
                    url=self.post_urls[0],
                    dont_filter = True,
                    formdata = self.formdata,
                    callback=self.parse,
                    headers = self.headers,
                    meta={'property_type': url.get('property_type'), "type": item}
                )

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//a[@class='blu']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        if page == 2 or seen:
            url = f"http://www.cscase.it/immobili.php?cond=%28inaffitto%3D{response.meta.get('type')}%29+AND+id_tipo%3D1+AND+categoria%3D1&affitto=1&vendita=0&pag={page}"
            yield Request(
                url,
                callback=self.parse, 
                meta={
                    "page": page+1, 
                    "property_type": response.meta.get('property_type'),
                    "type": response.meta.get('type')
                }
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)


        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        external_id=response.url
        if external_id:
            item_loader.add_value("external_id",external_id.split("=")[-1])

        title = "".join(response.xpath("//title//text()").get())
        if title:
            item_loader.add_value("title",title)

        address = response.xpath("//div[contains(.,'Indirizzo:')]//following-sibling::div[contains(.,'Indirizzo:')]//strong//text()").get()
        if address:
            item_loader.add_value("address",address)
        adrescheck=item_loader.get_output_value("address")
        if not adrescheck:
            adres=response.xpath("//div[.='Zona:']//following-sibling::div/strong/text()").get()
            adres2=response.xpath("//div[.='Sottozona:']//following-sibling::div/strong/text()").get()
            if adres and adres2:
                item_loader.add_value("address",adres+" "+adres2)
            
        city = response.xpath("//div[contains(.,'Zona:')]//following-sibling::div[contains(.,'Zona:')]//strong//text()").get()
        if city:
            item_loader.add_value("city",city)

        description = "".join(response.xpath("//div[contains(.,'Descrizione:')]//following-sibling::div[contains(.,'Descrizione:')]//strong//text()").get())
        if description:
            item_loader.add_value("description",description)

        rent = response.xpath("//div[contains(.,'Canone mensile:')]//following-sibling::div[contains(.,'Canone mensile:')]//strong//text()").get()
        if rent:
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")


        energy_label = response.xpath("//div[contains(.,'Classe energetica:')]//following-sibling::div[contains(.,'Classe energetica:')]//strong//text()").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label)

        square_meters = response.xpath("//div[.='Superficie:']//following-sibling::div/strong/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(" ")[0])
               

        bathroom_count = response.xpath(".//i[contains(@class,'fa fa-bath')]//following-sibling::p//text()").get()
        if bathroom_count:
            bathroom_count=bathroom_count.split(" ")[-1]
            item_loader.add_value("bathroom_count",bathroom_count)

        room_count = response.xpath(".//i[contains(@class,'fa fa-square')]//following-sibling::p//text()").get()
        if room_count:
            room_count=room_count.split(" ")[-1]
            item_loader.add_value("room_count",room_count)

        furnished = "".join(response.xpath("//div[contains(.,'Stato:')]//following-sibling::div[contains(.,'Stato:')]//strong//text()").get())
        if "Non arredato" in furnished:
            item_loader.add_value("furnished",False)
        else:
            item_loader.add_value("furnished",True)

        images = [response.urljoin(x)for x in response.xpath("//div[contains(@style,'text-align:center; position:absolute; top:0; height:100%; width:100%;')]//img//@src").extract()]
        if images:
                item_loader.add_value("images", images)

        item_loader.add_value("landlord_phone", " (+39) 011-5185225 / (+39) 011-5618118")
        item_loader.add_value("landlord_email", "info@cscase.it")
        item_loader.add_value("landlord_name", "Tarantino c.s. caseimmobiliare")
        
        yield item_loader.load_item()