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
    name = 'akimmoconseil_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    custom_settings = {
        "PROXY_FR_ON":"True",
    }

    def start_requests(self):
        start_urls = [
            {"url": "https://www.akimmoconseil.fr/fr/listing.html?numero=&loc=location&supplementaires=1&type%5B%5D=appartement&prixmin=&prixmax=&surfacemin=&surfacemax=&terrain=", "property_type": "apartment"},
	        {"url": "https://www.akimmoconseil.fr/fr/listing.html?loc=location&type%5B%5D=maison&surfacemin=&prixmax=&tri=prix-asc&page=1&coordonnees=&supplementaires=1&prixmin=&surfacemax=&terrain=&numero=&idpers=&options=&telhab=&piecemin=", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            })

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='header-item']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Akimmoconseil_PySpider_"+ self.country + "_" + self.locale)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        title = " ".join(response.xpath("//div/h1[@class='titre']/span//text()").extract())
        item_loader.add_value("title", title.strip())

        desc = "".join(response.xpath("//div[@id='descdetail']//div[@class='col-sm-8']//text()").extract())
        item_loader.add_value("description", desc.strip())
        external_id = "".join(response.xpath("//div[@id='detailinfosdetail']//li[span[contains(.,'Référence')]]/span[2]//text()").extract())
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        city = "".join(response.xpath("//div[@id='detailinfosdetail']//li[span[contains(.,'Ville')]]/span[2]/span/text()").extract())
        if city:
            item_loader.add_value("city", city.strip())

        address = response.xpath("//div[@class='info_ville']/text()").extract_first()
        if address:
            item_loader.add_value("address", address.strip())
        price =" ".join(response.xpath("//div[@id='detailinfosdetail']//li[span[contains(.,'Prix')]]/span[2]/span/text()").extract())
        if price:
            item_loader.add_value("rent_string", price.replace(" ","").replace('\xa0', ''))
    
        available_date="".join(response.xpath("//div[@id='descdetail']//div[@class='col-sm-8']//text()[contains(.,'Disponible ')]").extract())
        if available_date:
            date = available_date.replace("Disponible ","").replace("début ","")          
            date_parsed = dateparser.parse(date.strip(), date_formats=["%d-%m-%Y"], languages=['fr'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        square = "".join(response.xpath("//div[@id='detailinfosdetail']//li[span[contains(.,'Surface')]]/span[2]/span/text()").extract())
        if square:
            square = square.split("m")[0]
            item_loader.add_value("square_meters", square.strip())
        floor = "".join(response.xpath("//div[@id='detailinfosdetail']//li[span[contains(.,'étages')]]/span[2]/span/text()").extract())
        if floor:
            item_loader.add_value("floor", floor.strip())
        room_count = "".join(response.xpath("//div[@id='detailinfosdetail']//li[span[contains(.,'chambres')]]/span[2]/span/text()[normalize-space()]").extract())
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            room_count = "".join(response.xpath("//div[@id='detailinfosdetail']//li[span[contains(.,'pièces')]]/span[2]/span/text()[normalize-space()]").extract())
            if room_count:
                item_loader.add_value("room_count", room_count)
          
        script_map = response.xpath("//script[@type='text/javascript']/text()[contains(.,'lng:') and contains(.,'centerLngLat')]").extract_first()
        if script_map:
            latlng = script_map.split("centerLngLat")[1]
            item_loader.add_value("latitude", latlng.split("lat: '")[1].split("'")[0].strip())
            item_loader.add_value("longitude", latlng.split("lng: '")[1].split("'")[0].strip())
        
        deposit = response.xpath("//div[@id='descdetail']//div[@class='col-sm-8']//text()[contains(.,'Dépôt de garantie') and contains(.,'€')]").extract_first()
        if deposit:
            item_loader.add_value("deposit", deposit.split("Dépôt de garantie")[1].strip().split("€")[0].replace(" ","").replace('\xa0', ''))
   
        utilities = response.xpath("//div[@class='info_prix-hai']/text()[contains(.,'Charges :')]").extract_first()
        if utilities:
            item_loader.add_value("utilities", utilities.split(":")[1].strip().split("€")[0])

        parking = "".join(response.xpath("//div[@id='detailinfosdetail']//li[span[contains(.,'Parking')]]/span[2]/span/text()[normalize-space()]").extract())
        if parking:
            item_loader.add_value("parking", True)
        balcony = "".join(response.xpath("//div[@id='detailinfosdetail']//li[span[contains(.,'Nbr de balcon')]]/span[2]/span/text()[normalize-space()]").extract())
        if balcony:
            item_loader.add_value("balcony", True)

        images = [response.urljoin(x) for x in response.xpath("//div[@id='carouselImages']/div[@class='carousel-inner']//a/@href").extract()]
        if images:
            item_loader.add_value("images", images)       
        
        item_loader.add_value("landlord_phone", "09 50 01 58 44")
        item_loader.add_value("landlord_name", "AK Immo")
        item_loader.add_value("landlord_email", "agence@akimmoconseil.fr")

        yield item_loader.load_item()