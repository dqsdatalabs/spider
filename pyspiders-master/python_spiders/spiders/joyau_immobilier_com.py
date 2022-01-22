# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from python_spiders.loaders import ListingLoader
import re

class MySpider(Spider):
    name = 'joyau_immobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    post_url = "http://www.joyau-immobilier.com/fr/recherche/"
    current_index = 0
    other_prop = ["2"]
    other_prop_type = ["house"]
    def start_requests(self):
        self.formdata = {
            "nature": "2",
            "type[]": "1",
            "price":"" ,
            "reference":"" ,
            "age": "",
            "tenant_min": "",
            "tenant_max": "",
            "rent_type": "",
            "newprogram_delivery_at": "",
            "newprogram_delivery_at_display": "",
            "currency": "EUR",
            "customroute": "",
            "homepage": "",
        }
        yield FormRequest(
            url=self.post_url,
            callback=self.parse,
            dont_filter=True,
            formdata=self.formdata,
            meta={
                "property_type":"apartment",
            }
        )


    # 1. FOLLOWING
    def parse(self, response):
 
        for item in response.xpath("//ul/li//a[@class='button']/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
          
        next_page = response.xpath("//li[@class='nextpage']/a/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), dont_filter=True, callback=self.parse, meta={"property_type":response.meta["property_type"], "page":page+1})
        
        elif self.current_index < len(self.other_prop):
            self.formdata["type[]"] = self.other_prop[self.current_index]
            yield FormRequest(
                url=self.post_url,
                callback=self.parse,
                dont_filter=True,
                formdata=self.formdata,
                meta={
                    "property_type":self.other_prop_type[self.current_index],
                }
            )
            self.current_index += 1

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        
        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", title)
            item_loader.add_value("address", title.split(" - ")[1].strip())
            item_loader.add_value("city", title.split(" - ")[1].strip())
        
        room_count = response.xpath("//li[contains(.,'Chambres')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(" ")[0])
        
        room_count = response.xpath("//li[contains(.,'salle')]/text()").get()
        if room_count:
            item_loader.add_value("bathroom_count", room_count.split(" ")[0])
        
        rent = response.xpath("//li[contains(.,'Mois')]/text()[contains(.,'€')]").get()
        if rent:
            rent = rent.split("€")[0].strip().replace(" ","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        square_meters = response.xpath("//li[contains(.,'Surface')]/span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split(" ")[0])
        
        floor = response.xpath("//li[contains(.,'Etage')]/span/text()").get()
        if floor:
            item_loader.add_value("floor", floor)
        
        terrace = response.xpath("//li[contains(.,'Terrasse')]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        energy_label = response.xpath("//img[contains(@alt,'Consommation')]/@src").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split("/")[-1])
        
        utilities = response.xpath("//li[contains(.,'Charges')]/span/text()").get()
        if utilities:
            utilities = utilities.split("€")[0].strip()
            item_loader.add_value("utilities", utilities)
        
        deposit = response.xpath("//li[contains(.,'Dépôt')]/span/text()").get()
        if deposit:
            deposit = deposit.split("€")[0].strip()
            item_loader.add_value("deposit", deposit)
        
        external_id = response.xpath("//li[contains(.,'Ref')]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(".")[1].strip())
        
        description = " ".join(response.xpath("//p[@id='description']//text()").getall())
        if description:
            description = re.sub('\s{2,}', ' ', description.strip())
            item_loader.add_value("description", description)
        
        furnished = response.xpath("//li[contains(.,'Meublé')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        dishwasher = response.xpath("//li[contains(.,'Lave-vaisselle')]").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        
        washing_machine = response.xpath("//li[contains(.,'Lave-linge')]").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)
        
        images = [x for x in response.xpath("//a[@class='slideshow']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude_longitude = response.xpath("//script[contains(.,'L.marker([')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('L.marker([')[1].split(',')[0]
            longitude = latitude_longitude.split('L.marker([')[1].split(',')[1].split(']')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        item_loader.add_xpath("landlord_name", "//p[contains(@class,'userName')]/strong/text()")
        item_loader.add_xpath("landlord_phone", "//div[@class='userBlock']//span[contains(@class,'phone')]/a/text()")
        item_loader.add_xpath("landlord_email", "//div[@class='userBlock']//span[contains(@class,'mail')]/a/text()")
        
        yield item_loader.load_item()