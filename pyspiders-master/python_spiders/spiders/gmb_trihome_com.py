# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
class MySpider(Spider):
    name = 'gmb_trihome_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        url = "http://www.gmb-trihome.com/fr/recherche/"
        start_urls = [
            {
                "formdata" : {
                        'nature': '2',
                        'type[]': '1',
                        'currency': 'EUR'
                    },
                "property_type" : "apartment",
            },
            {
                "formdata" : {
                        'nature': '2',
                        'type[]': '2',
                        'currency': 'EUR'
                    },
                "property_type" : "house",
            },
        ]
        for item in start_urls:
            yield FormRequest(url, formdata=item["formdata"], dont_filter=True, callback=self.parse, meta={'property_type': item["property_type"]})

    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//li[contains(@class,'ad')]//a[contains(.,'Vue détaillée')]/@href").getall():
            seen = True
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type": response.meta["property_type"]})
        
        if page == 2 or seen:
            yield Request(f"http://www.gmb-trihome.com/fr/recherche/{page}", callback=self.parse, meta={"property_type": response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
 
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Gmb_Trihome_PySpider_france")
        
        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
          
        floor = response.xpath("//li[text()='Etage ']/span/text()").get()
        if floor:
            item_loader.add_value("floor", floor.split("/")[0].replace("ème","").strip())
        external_id = response.xpath("//section[@class='showPictures']//li[contains(.,'Ref. ')]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split("Ref.")[-1].strip())
        city = response.xpath("//section[@class='showPictures']//h2/text()[last()]").get()
        if city:
            item_loader.add_value("city", city.strip())
            item_loader.add_value("address", city)
         
        available_date = response.xpath("//li[text()='Disponible le ']/span/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split("Disponible le")[-1].strip(), date_formats=["%d/%m/%Y"], languages=['fr'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        rent = response.xpath("//section[@class='showPictures']//li[contains(.,' € ')]/text()").get()
        if rent:
            if rent.count("€")==1:
                item_loader.add_value("rent", int(float(rent.replace(" ","").replace(",",".").split("€")[0])))  
            else:
                rent = rent.split("€")[-2].strip().split(" ")[-1]
                item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        description = "".join(response.xpath("//p[@class='comment']//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        bathroom_count = response.xpath("//section[@class='showPictures']//li[contains(.,'salle de bain')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(" ")[0])
        room_count = response.xpath("//section[@class='showPictures']//li[contains(.,'chambres')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(" ")[0])
        else:
            room_count = response.xpath("//li[text()='Pièces ']/span/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.split(" ")[0])

        square_meters = response.xpath("//li[text()='Surface ']/span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].strip())
        deposit = response.xpath("//li[text()='Dépôt de garantie']/span/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.replace(" ","").strip())
        utilities = response.xpath("//li[text()='Charges']/span/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split(",")[0])

        parking = response.xpath("//li[contains(.,'Parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        furnished = response.xpath("//li[contains(.,'Meublé')]/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        terrace = response.xpath("//li[contains(.,'Terrasse')]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        dishwasher = response.xpath("//li[contains(.,'Lave-vaisselle')]/text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        washing_machine = response.xpath("//li[contains(.,'Lave-linge')]/text()").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)
        elevator = response.xpath("//li[contains(.,'Ascenseur')]/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        images = [response.urljoin(x) for x in response.xpath("//div[contains(@class,'show-carousel')]//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        script_map = response.xpath("//script[contains(.,'L.marker([')]/text()").get()
        if script_map:
            latlng = script_map.split("L.marker([")[-1].split("]")[0]
            item_loader.add_value("latitude", latlng.split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split(",")[1].strip())
        item_loader.add_xpath("landlord_name", "//div[@class='userBlock']/p/strong/text()")
        phone=response.xpath("//span[@class='phone smallIcon']/a[1]/text()").get()
        if phone:
            item_loader.add_value("landlord_phone",phone)
        item_loader.add_value("landlord_email", "gmb.immobilier@gmail.com")
        yield item_loader.load_item()