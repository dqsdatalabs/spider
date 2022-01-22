# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from datetime import datetime

class MySpider(Spider):
    name = 'nonsoloaffitti_com'
    execution_type='testing'
    country='italy'
    locale='it'  
 
    Custom_settings = {
        "PROXY_TR_ON": True,
        "CONCURRENT_REQUESTS" : 4,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,
        "HTTPERROR_ALLOWED_CODES": [301,302,400,401,406,403]
    }
 
    external_source = "Nonsoloaffitti_PySpider_italy"
    post_urls = ['http://nonsoloaffitti.com/website/cerca.php']  # LEVEL 1
    
    formdata = {
        "ad_type_id": "1;Affitto",
        "building_type_id": "1;Appartamento",
        "rooms": "",
        "square_meters": "",
        "rome_zone_id": "",
        "price": "", 
        "advs_search": "advs_search",
    }

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "1;Appartamento",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "5;Casa indipendente",
                    "26;Casale",
                    "9;Mansarda",
                    "19;Villa a schiera"
                ],
                "property_type": "house"
            },
            {
                "url": [
                    "10;Monolocale",
                ],
                "property_type": "studio"
            },
             {
                "url": [
                    "15;Stanza",
                ],
                "property_type": "room"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                self.formdata["building_type_id"] = item
                yield FormRequest(
                    url=self.post_urls[0],
                    callback=self.parse,
                    formdata=self.formdata,
                    meta={
                        'property_type': url.get('property_type'),
                        'type': item
                    }
                )

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen = False
        
        for item in response.xpath("//div[@class='ad_image']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            formdata = {
                "advs_search": "advs_search",
                "ad_type_id": "1;Affitto",
                "building_type_id": f"{response.meta.get('type')}",
                "rooms": "",
                "square_meters": "",
                "rome_zone_id": "",
                "price": "",
                "page": f"{page}",
            }
            yield FormRequest(
                self.post_urls[0],
                formdata=formdata,
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

        title = response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title", title)

        external_id = response.xpath("//ul[@class='list-unstyled']//li//label[contains(.,'Riferimento annuncio:')]//following-sibling::text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.replace("\t","").replace("\n",""))

        description = response.xpath("//h2[contains(.,'Descrizione')]//following-sibling::p//text()").get()
        if description:
            item_loader.add_value("description", description.split("per info")[0].split("Per info")[0])

        rent = response.xpath("//div[contains(@class,'col-sm-12 ad_detail_contact_us')]//following-sibling::a//text()").get()
        if rent:
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        square_meters = response.xpath("//ul[@class='list-unstyled']//li//label[contains(.,'Superficie')]//following-sibling::text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("mq")[0])
        furnished=response.xpath("//label[.='Arredato:']/following-sibling::text()").get()
        if furnished and "Si" in furnished:
            item_loader.add_value("furnished",True)
            

        bathroom_count = response.xpath("//ul[@class='list-unstyled']//li//label[contains(.,'Bagni')]//following-sibling::text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        energy_label = response.xpath("//ul[@class='list-unstyled']//li//label[contains(.,'Classe energetica:')]//following-sibling::text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)

        address = response.xpath("//ul[@class='list-unstyled']//li//label[contains(.,'Indirizzo:')]//following-sibling::text()").get()
        if address:
            item_loader.add_value("address", address)
            
        city = response.xpath("//ul[@class='list-unstyled']//li//label[contains(.,'Comune / Zona:')]//following-sibling::text()").get()
        if city:
            item_loader.add_value("city", city.replace("\t","").replace("\n",""))

        balcony = response.xpath("//ul[@class='list-unstyled']//li//label[contains(.,'Balconi:')]//following-sibling::text()").get()
        if 'no' in balcony:
            item_loader.add_value("balcony", False)
        else:
            item_loader.add_value("balcony", True)

        terrace = response.xpath("//ul[@class='list-unstyled']//li//label[contains(.,'Terrazza/e:')]//following-sibling::text()").get()
        if 'no' in terrace:
            item_loader.add_value("terrace", False)
        else:
            item_loader.add_value("terrace", True)

        parking = response.xpath("//ul[@class='list-unstyled']//li//label[contains(.,'Posto auto:')]//following-sibling::text()").get()
        if 'Assente' in parking:
            item_loader.add_value("parking", False)
        else:
            item_loader.add_value("parking", True)

        furnished = response.xpath("//ul[@class='list-unstyled']//li//label[contains(.,'Posto auto:')]//following-sibling::text()").get()
        if 'Si' in furnished:
            item_loader.add_value("furnished", True)
        else:
            item_loader.add_value("furnished",False)

        images = [response.urljoin(x) for x in response.xpath("//div[@class='row']//img[contains(@id,'shown_image')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        available_date=response.xpath("//label[.='Disponibile da:']/following-sibling::text()").get()
        if "subito" in available_date:
            available_date = datetime.now().strftime("%Y-%m-%d")
            item_loader.add_value("available_date", available_date)

        room_count  = response.xpath("//li/label[contains(.,'Locali')]/following-sibling::text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)

        floor  = response.xpath("//li/label[contains(.,'Piano')]/following-sibling::text()").get()
        if room_count:
            item_loader.add_value("floor", floor)

        item_loader.add_value("landlord_name", "Non Solo Affitti")
        item_loader.add_value("landlord_phone", "+39 06 44702256")
        item_loader.add_value("landlord_email", "ptmimmobiliare@tiscali.it")
        yield item_loader.load_item()