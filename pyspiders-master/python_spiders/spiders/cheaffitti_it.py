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
    name = 'cheaffitti_it'
    external_source = "Cheaffitti_PySpider_italy"
    execution_type='testing'
    country='italy'
    locale='it' 
    start_urls = ['https://cheaffitti.it/appartamenti_in_affitto.php']  # LEVEL 1
    form_data = {
        "agenzia": "",
        "NUMPAGE": "1",
        "ordina": "6",
        "provincia": "",
        "tipologia": "",
        "arredato": "",
        "camere": "",
        "canone": "",
    }
    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36"
    }
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "Appartamento",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "Casa indipendente",
                    "Casa Semindipendente",
                    "Villa o villino"
                ],
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                self.form_data["tipologia"] = item
                yield FormRequest(
                    url=self.start_urls[0],
                    dont_filter=True,
                    formdata=self.form_data,
                    headers=self.headers,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type'), "type": item}
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//div[@class='listing-title']//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(
                follow_url, 
                callback=self.populate_item, 
                meta={
                    "property_type": response.meta.get('property_type'),
                    "type": response.meta.get('type')
                }
            )
            seen = True
        
        if page == 2 or seen:
            self.form_data["NUMPAGE"] = f"0{page}"
            self.form_data["tipologia"] = response.meta.get('type')
            yield FormRequest(
                url=self.start_urls[0],
                dont_filter=True,
                formdata=self.form_data,
                headers=self.headers,
                callback=self.parse,
                meta={"page": page+1, 'property_type': response.meta.get('property_type'), "type": response.meta.get('type')}
            )
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        studio_check = response.xpath("(//div[@class='single-property-content']/p/text())[1]").get()
        if studio_check and "monolocale" in studio_check.lower():
            property_type = "studio"
        else:
            property_type = response.meta.get('property_type')
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("external_source", self.external_source)

        external_id = "".join(response.xpath("//h3[contains(.,'Rif.')]//text()").get())
        if external_id:
            external_id = external_id.split("Rif.")[-1].split("-")[0]
            item_loader.add_value("external_id",external_id)
            
        title = "".join(response.xpath("//title//text()").get())
        if title:
            item_loader.add_value("title",title)

        address = response.xpath("//h3[contains(.,'Rif.')]//text()").get()
        if address:
            city = address.split("Rif.")[1].split("-")[1]
            item_loader.add_value("city",city.replace("\n","").replace("\t",""))
            address=address.split("-")[1].split("-")[:2]
            item_loader.add_value("address",address)

        description = "".join(response.xpath("//p[contains(@align,'justify')]//text()").get())
        if description:
            item_loader.add_value("description",description)

        square_meters = response.xpath("//span[contains(@class,'meta-size')]//text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters)
    
        if property_type == "studio":
            item_loader.add_value("room_count", 1)
        else:
            room_count = response.xpath("//span[@class='meta-bedroom']/text()").get()
            if room_count:
               item_loader.add_value("room_count", room_count) 

        bathroom_count = response.xpath("//span[contains(@class,'meta-bathroom')]//text()").get()
        if bathroom_count:
            bathroom_count=bathroom_count.split(" ")[-1]
            item_loader.add_value("bathroom_count",bathroom_count)

        rent = response.xpath("//h3[contains(.,'Euro ')]//following-sibling::span//text()").get()
        if rent:
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")

        utilities = response.xpath("//p[@class='single-property-address'][contains(.,'condominiali')]/b/text()[contains(.,'Euro')]").get()
        if utilities:
            utilities = utilities.split(',')[0].strip()
            item_loader.add_value("utilities",utilities)

        energy_label = response.xpath("//h5[contains(@class,'single-property-title')]//following-sibling::p//text()").get()
        if energy_label:
            energy_label = energy_label.split("APE:")[1].split("-")[0]
            item_loader.add_value("energy_label",energy_label) 

        images = [response.urljoin(x)for x in response.xpath("//div[contains(@class,'es-carousel')]//ul//li//img//@src").extract()]
        if images:
                item_loader.add_value("images",images)

        item_loader.add_value("landlord_phone", "0182.58.521")
        landlord_email = response.xpath("//input[@name='emailagenzia']/@value").get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email)
        else:
            item_loader.add_value("landlord_email", "albenga@cheaffitti.it")
        item_loader.add_value("landlord_name", "Che Affitti")

        yield item_loader.load_item()