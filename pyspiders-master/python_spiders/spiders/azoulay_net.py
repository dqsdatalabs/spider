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
import re

class MySpider(Spider):
    name = 'azoulay_net'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Azoulay_PySpider_france'
    def start_requests(self):
        self.headers = {
            "content-type": "application/x-www-form-urlencoded",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
            "origin": "http://www.azoulay.net"
        }
       
        self.data = {
            "nature": "2",
            "type[]": "1",
            "price": "",
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
       
        url = "http://www.azoulay.net/fr/recherche/"        
        yield FormRequest(
            url,
            formdata=self.data,
            headers=self.headers,
            callback=self.parse,
            meta={"property_type":"apartment"},
        )
    
    def jump(self, response):
        
        self.data["type[]"] = "2"
        url = "http://www.azoulay.net/fr/recherche/"        
        yield FormRequest(
            url,
            formdata=self.data,
            headers=self.headers,
            dont_filter =True,
            callback=self.parse,
            meta={"property_type":"house"},
        )

    def parse(self, response):
        property_type = response.meta.get("property_type")
        for item in response.xpath("//ul[@class='ads']/li//a[contains(.,'Vue')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})

        pagination = response.xpath("//ul[@class='pager']/li[@class='nextpage']/a/@href").get()
        if pagination:
            yield Request(response.urljoin(pagination), callback=self.parse, meta={"property_type":property_type})
        else:
            if self.data["type[]"] == "1":
                yield Request("http://www.azoulay.net/fr/recherche/2", self.jump, dont_filter=True)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source",self.external_source)
        
        title = response.xpath("//title/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        external_id = "".join(response.xpath("//li[contains(.,'Ref')]/text()").getall())
        if external_id:
            item_loader.add_value("external_id", external_id.split(" ")[1].strip())

        room_count =response.xpath("//span[contains(.,'pièces')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split("pi")[0].strip())
        else:
            room_count = "".join(response.xpath("//div[@class='summary details']/ul/li[contains(.,'Pièces')]/span/text()").getall())
            if room_count:
                item_loader.add_value("room_count", room_count.split("pièce")[0].strip())


        bathroom_count = "".join(response.xpath("//article/div/ul/li[contains(.,'salle')]/text()").getall())
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split("salle")[0].strip())

        meters = "".join(response.xpath("//article/div/ul/li[contains(.,'m²')]/text()").getall())
        if meters:
            item_loader.add_value("square_meters", meters.split("m²'")[0].strip())

        # Latlng = "".join(response.xpath("substring-before(substring-after(//script[contains(.,'marker_map_2  ')]/text(),'L.marker(['),',')").getall())
        # if meters:
        #     item_loader.add_value("latitude", Latlng)
        #     item_loader.add_xpath("longitude", "substring-before(substring-after(substring-after(//script[contains(.,'marker_map_2 ')]/text(),'L.marker(['),', '),']')")

        utilities = "".join(response.xpath("//div[@class='legal details']/ul/li[contains(.,'Charges')]/span/text()").getall())
        if utilities:
            uti =  utilities.split("€")[0].replace(",",".").replace(" ","")
            item_loader.add_value("utilities",int(float(uti)))

        energy_label = response.xpath("substring-after(//div[@class='diagnostics details ']/img[contains(@alt,'Énergie - Consom')]/@src,'1/')").extract_first()
        if energy_label:
            if "%" in energy_label:
                label = energy_label.split("%")[0]
            else:
                label = energy_label.replace("1/","").strip()
            
            item_loader.add_value("energy_label", energy_label_calculate(label.replace("1/","").strip()))

        deposit = "".join(response.xpath("//div[@class='legal details']/ul/li[contains(.,'Dépôt de garantie')]/span/text()").getall())
        if deposit:
            deposit2 =  deposit.split("€")[0].replace(" ","").replace(",","")
            item_loader.add_value("deposit",int(float(deposit2)))

        rent = "".join(response.xpath("substring-before(//article/div/ul/li[contains(.,'€')]/text(),'/')").getall())
        if rent:
            price = rent.replace(" ","").strip().split("€")[0].replace(",",".")
            item_loader.add_value("rent", int(float(price)))
        item_loader.add_value("currency", "EUR")

        description = " ".join(response.xpath("//p[@id='description']/text()").getall()).strip()   
        if description:
            description = re.sub('\s{2,}', ' ', description.strip())
            item_loader.add_value("description", description.strip())

        floor = " ".join(response.xpath("//div[@class='summary details']/ul/li[contains(.,'Etage')]/span/text()").getall()).strip()   
        if floor:
            item_loader.add_value("floor", floor.split("/")[0].replace("ème","").replace("er","").strip())

        address = " ".join(response.xpath("//article/div/h2/text()[2]").getall()).strip()   
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.strip())

        available_date=response.xpath("//div[@class='summary details']/ul/li[contains(.,'Disponibilité ')]/span/text()[.!='Libre']").get()

        if available_date:
            date_parsed = dateparser.parse(
                available_date, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)

        servis =  " ".join(response.xpath("//div[@class='services details']/ul/li/text()").getall()).strip()
        if servis:
            if "Ascenseur" in servis:
                item_loader.add_value("elevator", True)

        balcony =  " ".join(response.xpath("//div[@class='areas details']/ul/li/text()[contains(.,'Balcon')]").getall()).strip()
        if balcony:
                item_loader.add_value("balcony", True)

        parking =  " ".join(response.xpath("//div[@class='areas details']/ul/li/text()[contains(.,'Parking')]").getall()).strip()
        if parking:
                item_loader.add_value("parking", True)

        images = [ response.urljoin(x) for x in response.xpath("//div[@class='show-carousel owl-carousel owl-theme']/div/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_xpath("landlord_phone", "normalize-space(//span[@class='phone smallIcon']/a/text())")
        item_loader.add_xpath("landlord_name", "normalize-space(//p[@class='smallIcon userName']/strong/text())")
        item_loader.add_xpath("landlord_email", "normalize-space(//span[@class='mail smallIcon']/a/text())")

        script_data = response.xpath("//script[contains(.,'marker')]/text()").get()
        if script_data:
            latlng = script_data.split("L.marker([")[1].split("]")[0]
            item_loader.add_value("latitude", latlng.split(",")[0])
            item_loader.add_value("longitude", latlng.split(",")[1])

        yield item_loader.load_item()

def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number <= 50:
        energy_label = "A"
    elif energy_number > 50 and energy_number <= 90:
        energy_label = "B"
    elif energy_number > 90 and energy_number <= 150:
        energy_label = "C"
    elif energy_number > 150 and energy_number <= 230:
        energy_label = "D"
    elif energy_number > 230 and energy_number <= 330:
        energy_label = "E"
    elif energy_number > 330 and energy_number <= 450:
        energy_label = "F"
    elif energy_number > 450:
        energy_label = "G"
    return energy_label