# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request
from python_spiders.loaders import ListingItem
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import re

class MySpider(Spider):
    name = "pandomo_nl"
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl' # LEVEL 1

    def start_requests(self):
        start_urls = [
            {"url": "https://www.pandomo.nl/huurwoningen/?filter-group-id=10&filter%5B40%5D%5B%5D=Appartement", "property_type": "apartment"},
            {"url": "https://www.pandomo.nl/huurwoningen/?filter-group-id=10&filter%5B40%5D%5B%5D=Woonhuis", "property_type": "house"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            "base_url":url.get('url')})

    # 1. FOLLOWING LEVEL 1
    def parse(self, response):
        for item in response.xpath("//ol[@class='results']/li"):
            url = item.xpath("./a/@href").get()
            rented = item.xpath("./a//span[contains(.,'verhuurd')]").get()
            if not rented:
                yield response.follow(response.urljoin(url), self.populate_item, meta={'property_type': response.meta.get('property_type')})
        yield from self.paginate(response)

    # 2. SCRAPING LEVEL 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        if "statusCode=404" in response.url:
            return
        available_status = response.xpath("//div[@id='aniimated-thumbnials']/a[@class='object-media__item']/span/text()[.!='verhuurd']").extract_first()
        if available_status:
            item_loader.add_value("external_source", "Pandomo_PySpider_" + self.country + "_" + self.locale)

            title = response.xpath("//h1[contains(@class,'object-header__title')]/text()").get()
            if title:
                title = re.sub('\s{2,}', ' ', title.strip())
                item_loader.add_value("title", title)

            item_loader.add_value("external_link", response.url)

            external_id = response.url.split('-')[-1].strip().strip('/')
            if external_id:
                item_loader.add_value("external_id", external_id)

            item_loader.add_value("landlord_email", 'info@pandomo.nl')
            
            desc = "".join(response.xpath("//div[@class='content-readmore content']/text()").extract())
            if desc:
                desc = re.sub('\s{2,}', ' ', desc)
                item_loader.add_value("description", desc.strip())
                if "Balkon" in desc:
                    item_loader.add_value("balcony", True)
                if "Gestoffeerd" in desc:
                    item_loader.add_value("furnished", True)
                if "Lift " in desc:
                    item_loader.add_value("elevator", True)
                if 'geen huisdieren' in desc.lower():
                    item_loader.add_value("pets_allowed", False)
                if 'parkeren' in desc.lower():
                    item_loader.add_value("parking", True)
                if 'terras' in desc.lower(): 
                    item_loader.add_value("terrace", True)
                if 'vaatwasser' in desc.lower():
                    item_loader.add_value("dishwasher", True)
                if ' wasmachine' in desc.lower():
                    item_loader.add_value("washing_machine", True)

            address = response.xpath("//h2[@class='object-header__address']/text()").get()
            if address:
                item_loader.add_value("address", address.strip())
            zipcity=response.xpath("//h2[@class='object-header__address']/text()").get()
            if zipcity:
                item_loader.add_value("city",zipcity.split(" ")[1:])
                item_loader.add_value("zipcode",zipcity.split(" ")[0])
            
            latLng = response.xpath("//div[@class='map-canvas']/@data-geo").get()
            if latLng:
                latitude = latLng.split(",")[0]
                longitude = latLng.split(",")[1]
                if latitude and longitude:
                    item_loader.add_value("latitude", latitude)
                    item_loader.add_value("longitude", longitude)
        
            item_loader.add_value("property_type", response.meta.get('property_type'))
            
            square_meters = response.xpath("//th[contains(.,'Woonoppervlakte')]/parent::*/td/text()").get()
            if square_meters:
                item_loader.add_value("square_meters", square_meters.split(" ")[0])
            

            room_count = response.xpath("//th[contains(.,'Kamers')]/parent::*/td/text()").get()
            if room_count:
                item_loader.add_value("room_count",room_count.split(" ")[0])
            
            images = [response.urljoin(x) for x in response.xpath("//div[@class='fotoalbum object-media-wrap']/a/@href").getall()]
            if images:
                item_loader.add_value("images", images)
                item_loader.add_value("external_images_count", len(images))

            price = response.xpath("normalize-space(//div[@class='object-header__actions__price text-secondary']/text())").get()
            if price:
                item_loader.add_value("rent_string", price)
            item_loader.add_value("currency", "EUR")
            

            floor_plan_images = [response.urljoin(x) for x in response.xpath("//span[@class='va-middle']/img/@src").getall()]
            if floor_plan_images:
                item_loader.add_value("floor_plan_images", floor_plan_images)
                

            energy_label = response.xpath("//th[contains(.,'Energie')]/parent::*/td/text()").get()
            if energy_label and energy_label not in ["", " ", "-", "+"]:
                item_loader.add_value("energy_label", energy_label)

            
            landlord_name = response.xpath("//h4[@class='agent__name']/span/text()").get()
            if landlord_name and landlord_name not in ["", " ", "-"]:
                item_loader.add_value("landlord_name", landlord_name)
            
            landlord_phone = response.xpath("//a[@class='btn whatsapp-button btn--block m-b-5']/@href").get()
            if landlord_phone and landlord_phone not in ["", " ", "-"]:
                item_loader.add_value("landlord_phone", landlord_phone.split("&")[0].split("phone=")[1])
            
            yield item_loader.load_item()

    # 3. PAGINATION LEVEL 1
    def paginate(self, response):
        next_page_urls = response.css(
            "li.pager__item a::attr(href)"
        ).extract()  # pagination("next button") <a> element here
        if next_page_urls:
            for next_page_url in next_page_urls:
                yield response.follow(next_page_url, self.parse, meta={'property_type': response.meta.get('property_type')})
