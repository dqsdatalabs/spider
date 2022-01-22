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
    name = 'pianetacasamarsala_it'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Pianetacasamarsala_PySpider_italy"

    custom_settings = {
        "PROXY_TR_ON": True
    }
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "http://www.pianetacasamarsala.it/elenco_immobili.asp?tipo=14&pag=1",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "http://www.pianetacasamarsala.it/elenco_immobili.asp?tipo=12&pag=1",
                    "http://www.pianetacasamarsala.it/elenco_immobili.asp?tipo=13&pag=1"
                    ""
                ],
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//tr[contains(@class,'elenco-item')]//td[@width='450']//@href").getall():
            item = item.split("'")[1]
            follow_url = f"http://www.pianetacasamarsala.it/dati_immobile.asp{item}"
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
        
        next_page = response.xpath("//a[contains(@title,'Successiva')]/@href").get()
        if next_page:
            next_page = next_page.split("'")[1]
            url = f"http://www.pianetacasamarsala.it/elenco_immobili.asp{next_page}"
            yield Request(url, callback=self.parse, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        external_id=response.xpath("//td[contains(@style,'text-align: right')]//following-sibling::td[1]//p//text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.replace("\r","").replace("\t","").replace(" ",""))

        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)

        available_date=response.xpath("//td[contains(@style,'text-align: right')]//following-sibling::td[3]//p//text()").get()
        if available_date:
            item_loader.add_value("available_date",available_date.replace("\r","").replace("\t","").replace(" ",""))

        rent=response.xpath("//p[contains(@style,'width: 120px')]//text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[1])
        item_loader.add_value("currency","EUR")

        square_meters=response.xpath("//p[contains(@style,'width: 40px; height: 20px')]//text()[1]").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters)

        bathroom_count=response.xpath("//table[@id='table4']//tr//td[contains(.,'n.')]//following-sibling::td//p//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)

        address=response.xpath("//p[contains(@style,'width: 480px')]//text()").get()
        if address:
            item_loader.add_value("address",address)

        parking=response.xpath("//td[contains(.,'Posto')]//following-sibling::td[@width='50']//p[contains(@style,'width: 40px; height: 20px')]//text()[contains(.,'SI')]").get()
        if parking and "si" in parking.lower():
            item_loader.add_value("parking",True)
        else:
            item_loader.add_value("parking",False)

        description=response.xpath("//p[contains(@style,'width: 550px')]//text()[1]").getall()
        if description:
            item_loader.add_value("description",description)

        images = [response.urljoin(x)for x in response.xpath("//td[@style='height: 90%']//img[contains(@border,'0')]//@src").extract()]
        if images:
                item_loader.add_value("images", images)

        item_loader.add_value("landlord_phone", "0923.951904 - 329.8829132")
        item_loader.add_value("landlord_email", "info@pianetacasamarsala.it")
        item_loader.add_value("landlord_name", "Pianeta Casa")
        yield item_loader.load_item()