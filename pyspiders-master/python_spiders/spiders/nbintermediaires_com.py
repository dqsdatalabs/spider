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
    name = 'nbintermediaires_com' 
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.nbintermediaires.com/catalog/advanced_search_result.php?action=update_search&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_27_tmp=1&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_33_MAX=&C_30_MIN=&C_38_MIN=&C_38_search=COMPRIS&C_38_type=NUMBER&C_38_MAX=&search_id=1698819192597674&&search_id=1698819192597674",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.nbintermediaires.com/catalog/advanced_search_result.php?action=update_search&search_id=1698819192597674&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=2&C_27_tmp=2&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&keywords=&C_33_MAX=&C_30_MIN=&C_38_MIN=&C_38_search=COMPRIS&C_38_type=NUMBER&C_38_MAX=",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base_url":item})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@id='listing_bien']//div/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True

        page = response.xpath("//div[@class='barre_navigation nav_bottom']//ul[@class='list-pagination']/li[contains(@class,'next-link')]/a/@href").extract_first()
        if page:
            yield Request(
                response.urljoin(page),
                callback=self.parse,
                meta={"property_type":response.meta["property_type"]}
            )     
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        studio = response.xpath("//h1/text()").extract_first()
        if "studio" in studio.lower():
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Nbintermediaires_PySpider_france")

        external_id = response.xpath("//span[@itemprop='name']/text()[contains(.,'Ref')]").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())

        address = response.xpath("//div[@class='product-localisation']/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split(' ')[-1])
            item_loader.add_value("zipcode", address.strip().split(' ')[0])
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        description = " ".join(response.xpath("//div[@class='product-description']/text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        square_meters = response.xpath("//ul[@class='list-criteres']/li/div[@class='value']/text()[contains(.,'m²')]").get()
        if square_meters:
            square_meters =square_meters.split("-")[-1].strip().split("m")[0].replace(",",".")
            item_loader.add_value("square_meters", int(float(square_meters)))

        room_count = response.xpath("//ul[@class='list-criteres']/li/div[@class='value']/text()[contains(.,'pièce')]").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip().split(' ')[0].strip())
        elif response.meta.get('property_type') == "studio":
            item_loader.add_value("room_count", "1")
        else:
            room_count = response.xpath("//ul[@class='list-criteres']/li/div[@class='value']/text()[contains(.,'pi')]").get()
            if room_count:
                item_loader.add_value("room_count", room_count.strip().split(' ')[0].strip())

        
        bathroom_count = response.xpath("//ul[@class='list-criteres']/li/div[@class='value']/text()[contains(.,'salle')]").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip().split(' ')[0].strip())

        rent = response.xpath("//div[@class='product-price']/div/span[@class='alur_loyer_price']/text()").get()
        if rent:
            rent = rent.split("€")[0].strip().split(" ")[-1].strip().replace(',', '.').replace('\xa0', '')
            item_loader.add_value("rent", str(int(float(rent))))
        item_loader.add_value("currency", 'EUR')

        images = [x for x in response.xpath("//div[@id='slider_product_vignettes']/div/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        from datetime import datetime
        from datetime import date
        import dateparser
        available_date = response.xpath("//dt[contains(.,'Date de publication')]/following-sibling::dd[1]/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"], languages=['fr'])
            today = datetime.combine(date.today(), datetime.min.time())
            if date_parsed:
                result = today > date_parsed
                if result == True:
                    date_parsed = date_parsed.replace(year = today.year + 1)
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        deposit = response.xpath("//text()[contains(.,'Dépôt de garantie')]").get()
        if deposit:
            item_loader.add_value("deposit", "".join(filter(str.isnumeric, deposit.split('garantie')[1].split('.')[0].strip())))

        energy_label = response.xpath("//text()[contains(.,'Classe énergie')]").get()
        if energy_label:
            if energy_label.split(':')[-1].strip().upper() in ['A', 'B', 'C', 'D', 'E', 'F', 'G']:
                item_loader.add_value("energy_label", energy_label.split(':')[-1].strip().upper())
        
        floor = response.xpath("//text()[contains(.,'Etage')]").get()
        if floor:
            item_loader.add_value("floor", floor.split(':')[-1].split('/')[0].strip())

        utilities = response.xpath("//text()[contains(.,'Charges')]").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split(':')[-1].split('€')[0].split('.')[0].strip())
        
        parking = response.xpath("//dt[contains(.,'Parking')]/following-sibling::dd[1]/text()").get()
        if parking:
            if parking.strip().lower() == 'oui':
                item_loader.add_value("parking", True)
            elif parking.strip().lower() == 'non':
                item_loader.add_value("parking", False)

        balcony = response.xpath("//li[contains(text(),'Balcon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        elevator = response.xpath("//li[contains(text(),'Ascenceur')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        item_loader.add_value("landlord_name", "N & B Intermediaires")
        item_loader.add_value("landlord_phone", "01 42 24 42 21")

        if response.url !='https://www.nbintermediaires.com/':
            yield item_loader.load_item()