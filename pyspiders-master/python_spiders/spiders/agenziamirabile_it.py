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
    name = 'agenziamirabile_it'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Agenziamirabile_PySpider_italy"
    start_urls = ['https://www.agenziamirabile.it/darts_mir_home.php?idt=2&ltipo=Affittasi']  # LEVEL 1

    formdata = {
        "idcategoria": "14",
        "cerca": "cerca"
    }

    def start_requests(self):
        start_urls = [
            {
                "type": [
                    "13", "14", "15", "16", "21",
                ],
                "property_type": "apartment"
            },
	        {
                "type": [
                    "17", "24", "27"
                ],
                "property_type": "house"
            },
            {
                "type": [
                    "2"
                ],
                "property_type": "studio"
            }
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('type'):
                self.formdata["idcategoria"] = item
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
        
        for item in response.xpath("//div[@class='text1']/a/@href[contains(.,'idannuncio')]").extract():
            follow_url = f"https://www.agenziamirabile.it/darts_mir_home.php{item}"
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)


        external_id=response.xpath("//td//text()[contains(.,'Riferimento:')]").get()
        if external_id:
            external_id="".join(external_id.split("Riferimento:")[1])
            item_loader.add_value("external_id",external_id)

        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title.replace("\u2013",""))

        rent=response.xpath("//img[@src='darts_mir_icoprezzo.gif']//following-sibling::text()[1]").get()
        if rent:
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")
        address=response.xpath("//img[@src='darts_mir_icozona.gif']//following-sibling::b/text() | //img[@src='darts_mir_icozona.gif']//following-sibling::b/font/text()").get()
        if address:
            item_loader.add_value("address",address)
        item_loader.add_value("city","Palermo")
        square_meters=response.xpath("//img[@src='darts_mir_icoquadratura.gif']//following-sibling::text()[1]").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("+")[0])

        address=response.xpath("//ul[@class='caratteristiche']//li//span[contains(.,'Zona:')]//following-sibling::text()").get()
        if address:
            item_loader.add_value("address",address)

        room_count=response.xpath("//img[@src='darts_mir_icocategoria.gif']//following-sibling::text()").get()
        if room_count:
            room_count=room_count.split(" ")[-1]
            if room_count and "bilocale" in room_count:
                item_loader.add_value("room_count","2")
            if room_count and "quadrilocale" in room_count:
                item_loader.add_value("room_count","4")
            if room_count and "trilocale" in room_count:
                item_loader.add_value("room_count","3")
        floor=response.xpath("//img[@src='darts_mir_icoscala.gif']//following-sibling::text()").get()
        if floor:
            item_loader.add_value("floor",floor.split("°")[0].strip())

        energy_label=response.xpath("//img[@src='darts_mir_icoclenergetica.gif']//following-sibling::b//text()[1]").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label)

        description="".join(response.xpath("//p[@align='left']//b//text()[1] | //p[@align='left']//text()").getall())
        if description:
            item_loader.add_value("description",description.split(" prenota il tuo appuntamento ")[0].split("per info tel")[0].split("per maggiori informazioni")[0].split("per info")[0])

        images = [response.urljoin(x)for x in response.xpath("//a[contains(@class,'light')]//img//@src").extract()]
        if images:
                item_loader.add_value("images", images)

        item_loader.add_value("landlord_phone", "091 304377")
        item_loader.add_value("landlord_name", "Agenzia Mirabile")

        yield item_loader.load_item()