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
    name = 'dmimmo_com'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    external_source = 'Dmimmo_PySpider_france'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.dmimmo.com/annonces-locations/179-location-appartement.htm?p=1",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.dmimmo.com/annonces-locations/179-location-maison.htm?p=1",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='inner']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})
            seen = True
        
        if page == 2 or seen:
            p_url = response.url.split("?p=")[0] + f"?p={page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={'property_type': response.meta['property_type'], "page":page+1}
            )
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", self.external_source)

        external_id = response.xpath("//span[@class='reference']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        city = response.xpath("//div[@id='mapContainer']/meta[@itemprop='addresslocality']/@content").get()
        if city:
            item_loader.add_value("city", city)
        zipcode = response.xpath("//div[@id='mapContainer']/meta[@itemprop='postalcode']/@content").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode)
        address = response.xpath("//div[@id='mapContainer']/meta[@itemprop='streetAddress']/@content").get()
        if address:
            item_loader.add_value("address", address+", "+ city+", "+ zipcode)
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        description = " ".join(response.xpath("//div[@class='description']/p//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        square_meters = response.xpath("//span[contains(.,'Surface')]/following-sibling::text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split('m')[0].split('.')[0].strip())

        room_count = "".join(response.xpath("//div[@class='caracteristique']/div/ul/li[span[contains(.,'Nombre de chambre(s)')]]/text()[normalize-space()]").getall())
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        else:
            room_count = "".join(response.xpath(" //div[@class='caracteristique']/div/ul/li[span[contains(.,'Nombre de pi')]]/text()[normalize-space()] ").getall())
            if room_count:
                item_loader.add_value("room_count", room_count.strip()) 
        
        bathroom_count = "".join(response.xpath("//div[@class='caracteristique']/div/ul/li[span[contains(.,'Nombre de salle(s) de bain')]]/text()[normalize-space()]").getall())
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        else:
            bathroom_count = "".join(response.xpath("//div[@class='caracteristique']/div/ul/li[span[contains(.,'Nombre de salle(s)') and contains(.,'eau')]]/text()[normalize-space()]").getall())
            if bathroom_count:
                item_loader.add_value("bathroom_count", bathroom_count.strip())
            else:
                bathroom_count = response.xpath("//div[@class='caracteristique']/div/ul/li[span[contains(.,'Nombre de salle(s)')]]/text()[normalize-space()]").get()
                if bathroom_count:
                    item_loader.add_value("bathroom_count", bathroom_count.strip())
 
        rent = response.xpath("//h2[@class='prix']/text()").get()
        if rent:
            rent = rent.split('€')[0].strip().replace(' ', '').replace('\xa0', '')
            item_loader.add_value("rent", str(int(float(rent))))
            item_loader.add_value("currency", 'EUR')

        available_date="".join(response.xpath("//span[contains(.,'Disponible')]/following-sibling::text()").getall())

        if available_date:
            date2 =  available_date.replace("immédiatement","now").strip()
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)
        
        deposit = response.xpath("//span[contains(.,'Dépôt de garantie')]/following-sibling::text()[1]").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split('€')[0].strip().replace(' ', '').strip())
        
        images = [response.urljoin(x) for x in response.xpath("//div[@id='carousel-slider']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        latitude = response.xpath("//a[contains(@href,'centerLat')]/@href").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split('centerLat=')[1].split('&')[0].strip())
            item_loader.add_value("longitude", latitude.split('centerLng=')[1].split('&')[0].strip())
        
        energy_label = response.xpath("//span[@class='dpe']/div[@class='right']/span/@class").get()
        if energy_label:
            if energy_label.strip().upper() in ['A', 'B', 'C', 'D', 'E', 'F', 'G']:
                item_loader.add_value("energy_label", energy_label.strip().upper())
        
        floor = response.xpath("//span[contains(.,'Etage')]/following-sibling::text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())

        utilities = response.xpath("normalize-space(//h4[contains(.,'Informations sur le prix')]/..//text()[contains(.,' dont ')])").get()
        if utilities:
            item_loader.add_value("utilities", "".join(filter(str.isnumeric, utilities.split('dont ')[-1].split('€')[0].strip())))
        
        parking = response.xpath("//li[contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//li[contains(.,'Balcon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        furnished = response.xpath("//span[contains(.,'Meublé')]/following-sibling::text()").get()
        if furnished:
            if furnished.strip().lower() == 'oui':
                item_loader.add_value("furnished", True)
            elif furnished.strip().lower() == 'non':
                item_loader.add_value("furnished", False)

        elevator = response.xpath("//li[contains(.,'Ascenseur')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        terrace = response.xpath("//li[contains(.,'Terrasse')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        item_loader.add_value("landlord_name", "DURAND MONTOUCHÉ IMMOBILIER")
        item_loader.add_value("landlord_phone", "02 38 62 99 89")
      
        yield item_loader.load_item()