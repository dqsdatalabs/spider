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
    name = 'cityloger_fr'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    custom_settings = {"HTTPCACHE_ENABLED": False}

    def start_requests(self):
        url = "https://www.cityloger.fr/composants/_json_locations.php"
        headers = {
            'Connection': 'keep-alive',
            'Content-Length': '0',
            'Accept': 'text/html, */*; q=0.01',
            'X-Requested-With': 'XMLHttpRequest',
            'Origin': 'https://www.cityloger.fr',
            'Referer': 'https://www.cityloger.fr/logements-a-louer',
            'Accept-Language': 'tr,en;q=0.9',
        }
        yield FormRequest(url, headers=headers, callback=self.parse)
    
    # 1. FOLLOWING
    def parse(self, response):
        
        data = json.loads(response.body)
        for item in data:
            property_type = "apartment" if "appartement" in item["title"].lower() else "house"
            follow_url = "https://www.cityloger.fr/" + item["link"]
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        title = response.xpath("//div[contains(@class,'h-text')]/text()").get()
        if title:
            title = title.strip().replace("\t","")
            item_loader.add_value("title", title)
              
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Cityloger_PySpider_france")

        external_id = response.url.split('-')[-1]
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        address = " ".join(response.xpath("//div[@class='cardAddress']//text()").getall()).strip()
        if address:
            zipcode = address.split(' ')[0].strip()
            city = response.xpath("//div[@class='cardAddress']//h1//text()").get()
            city = city.split(zipcode)[1]
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
            

        description = " ".join(response.xpath("//div[@class='description text-justify']//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))
            if 'salle de bain' in description.lower():
                bathroom_count = description.lower().split('salle de bain')[0].split(',')[-1].strip()
                if bathroom_count.isnumeric():
                    item_loader.add_value("bathroom_count", bathroom_count)
                else:
                    item_loader.add_value("bathroom_count", '1')

        room_count = response.xpath("//ul[contains(@class,'features')]//div[contains(.,'pièce')]//text()").get()
        if room_count:
            room_count = room_count.split(" ")[0]
            item_loader.add_value("room_count", room_count)

        square_meters = response.xpath("//span[@class='icon-frame']/following-sibling::div/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split('m')[0].strip())

        rent = response.xpath("//div[@class='custom-figPrice-detail']/text()").get()
        if rent:
            rent = rent.split('€')[0].strip().replace(' ', '').replace('\xa0', '')
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", 'EUR')
        
        images = [response.urljoin(x) for x in response.xpath("//div[@class='carousel-inner']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        floor_plan_images = [response.urljoin(x) for x in response.xpath("//div[@id='plan']//img/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        latitude = response.xpath("//script[contains(.,'LatLng')]/text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split('setCenter(new google.maps.LatLng(')[1].split(',')[0].strip())
            item_loader.add_value("longitude", latitude.split('setCenter(new google.maps.LatLng(')[1].split(',')[1].split(')')[0].strip())
        
        energy_label = response.xpath("//img[contains(@src,'dpe')]/@src").get()
        if energy_label:
            energy_label = int(energy_label.split('-')[-2].strip())
            if energy_label >= 92:
                item_loader.add_value("energy_label", 'A')
            elif energy_label >= 81 and energy_label <= 91:
                item_loader.add_value("energy_label", 'B')
            elif energy_label >= 69 and energy_label <= 80:
                item_loader.add_value("energy_label", 'C')
            elif energy_label >= 55 and energy_label <= 68:
                item_loader.add_value("energy_label", 'D')
            elif energy_label >= 39 and energy_label <= 54:
                item_loader.add_value("energy_label", 'E')
            elif energy_label >= 21 and energy_label <= 38:
                item_loader.add_value("energy_label", 'F')
            elif energy_label >= 1 and energy_label <= 20:
                item_loader.add_value("energy_label", 'G')
        
        floor = response.xpath("//span[@class='fa fa-arrows-v']/following-sibling::div/text()").get()
        if floor:
            floor = "".join(filter(str.isnumeric, floor.split('etage')[0].strip()))
            if floor:
                item_loader.add_value("floor", floor)

        utilities = response.xpath("//br/following-sibling::text()[contains(.,'Charges')]").get()
        if utilities:
            utilities = utilities.split('€')[0].strip().split(' ')[-1].replace(',', '.')
            try:                
                item_loader.add_value("utilities", str(int(float(utilities))))
            except:
                pass
        
        parking = response.xpath("//td[contains(.,'Parkings souterrains')]/following-sibling::td/span/text()").get()
        if parking:
            if int(parking) > 0:
                item_loader.add_value("parking", True)
            elif int(parking) == 0:
                item_loader.add_value("parking", False)

        elevator = response.xpath("//td[contains(.,'Ascenseur')]/following-sibling::td/span/text()").get()
        if elevator:
            if elevator.strip().lower() == 'oui':
                item_loader.add_value("elevator", True)
            elif elevator.strip().lower() == 'non':
                item_loader.add_value("elevator", False)

        item_loader.add_value("landlord_name", "3F Groupe ActionLogement")

        landlord_email = response.xpath("//input[contains(@name,'email')]//@value").get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email)

        yield item_loader.load_item()