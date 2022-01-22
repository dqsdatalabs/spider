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
    name = 'intermediagruppocaltagirone_it'
    execution_type='testing'
    country='italy'
    locale='it' 
    external_source = "Intermediagruppocaltagirone_PySpider_italy"
    #start_urls = ['https://www.intermediagruppocaltagirone.it/immobiliare-case-in-affitto-roma']  
    post_url = "https://www.intermediagruppocaltagirone.it/web/load.asp" # LEVEL 1
    current_index = 0
    # 1. FOLLOWING
    def start_requests(self):
        formdata = {
            "loaded_max": "2",
            "c": "1",
            "contratto": "2",
            "a": "1",
            "tipoimmobile": "",
            "prezzo_min": "450",
            "prezzo_max": "910",
            "sup_min": "0",
            "sup_max": "94",
        }
        yield FormRequest(
            url=self.post_url,
            callback=self.parse,
            dont_filter=True,
            formdata=formdata,
            meta={
                "property_type":"apartment",
            }
        )
    
    def parse(self, response):

        seen = False
        for item in response.xpath("//div[contains(@class,'boxbox')]/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        
        if seen == True:
            self.current_index += 1
            formdata = {
                "loaded_max": str(self.current_index),
                "c": "1",
                "contratto": "2",
                "a": "1",
                "tipoimmobile": "",
                "prezzo_min": "450",
                "prezzo_max": "910",
                "sup_min": "0",
                "sup_max": "94",
            }
            yield FormRequest(
                url=self.post_url,
                callback=self.parse,
                dont_filter=True,
                formdata=formdata,
                meta={
                    "property_type":"apartment",
                }
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        prop_check = "".join(response.xpath("//title//text()").getall())
        if prop_check and "negozio" in prop_check.lower():
            return
        elif prop_check and "monolocale" in prop_check.lower():
            property_type = "studio"
        else:
            property_type = response.meta.get('property_type')
        
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("external_source", self.external_source)


        title = response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title", title)

        external_id = response.url
        if external_id:
            item_loader.add_value("external_id", external_id.split("-")[-1].strip())

        description = "".join(response.xpath("//div[@class='col-xl-8 col-lg-7 p-0']/p/text()").getall())
        if description:
            item_loader.add_value("description", description.strip())

        rent = response.xpath("//div[@class='box-prezzo ']/p/span/text()[contains(.,'€')]").get()
        if rent:
            item_loader.add_value("rent", rent.replace("€","").strip())
        item_loader.add_value("currency", "EUR")

        if property_type == "studio":
            item_loader.add_value("room_count", 1)
        else:
            room_count = response.xpath("//span[contains(.,'locali')]/parent::p/span[1]/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count)
        bathroom_count = response.xpath("//span[contains(.,'bagno')]/parent::p/span[1]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        square_meters = response.xpath("//span[contains(.,'superficie')]/parent::p/span[1]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split(',')[0].strip())

        city = response.xpath("//title//text()").get()
        if city:
            city2 = city.split('affitto a ')[-1].split('-')[0].strip()
            if city2:
                item_loader.add_value("city", city2.split('affitto a ')[-1].split('-')[0].strip())

        address = response.xpath("//p[contains(@class,'titolo-dettaglio')]/span/text()").get()
        if address:
            if city2:
                address = address + ", " + city2
            item_loader.add_value("address", address)

        energy_label = response.xpath("//span[contains(.,'energetica')]/parent::p/span[1]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)

        terrace = response.xpath("//td[contains(.,'Terrazzo')]/following-sibling::td/text()").get()
        if terrace and "si" in terrace.lower():
            item_loader.add_value("terrace", True)

        elevator = response.xpath("//td[contains(.,'Ascensore')]/following-sibling::td/text()").get()
        if elevator and "si" in elevator.lower():
            item_loader.add_value("elevator", True)
        
        parking = response.xpath("//td[contains(.,'Posto auto')]/following-sibling::td/text()").get()
        if parking and "si" in parking.lower():
            item_loader.add_value("parking", True)

        images = [response.urljoin(x) for x in response.xpath("//div[@id='tab2']//div[@class='center-planimetria']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        floor_plan_images = [response.urljoin(x) for x in response.xpath("//div[@id='tab1']//div[@class='center-planimetria']/a/@href").extract()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        item_loader.add_value("landlord_name", "INTERMEDIA GRUPPO CALTAGIRONE")
        item_loader.add_value("landlord_phone", "+39 06 454 122 47")
        item_loader.add_value("landlord_email", "info@intermediagruppocaltagirone.it")
        yield item_loader.load_item()