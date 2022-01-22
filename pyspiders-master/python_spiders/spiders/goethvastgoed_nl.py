# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request
from python_spiders.loaders import ListingItem
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import dateparser 
import re

class MySpider(Spider):
    name = "goethvastgoed_nl"
    execution_type = 'testing'
    country = 'netherlands' 
    locale = 'nl' 
    external_source='Goethvastgoed_PySpider_netherlands_nl'
    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.goethvastgoed.nl/nl/Aanbod?filter=house_filter&_token=9N5pjCgPW0vTY1ay5YUTQznN5DSqahC8qwIiXyDY&city=&type=house&min_price=&max_price=",
                "property_type" : "house"
            },
            {
                "url" : "https://www.goethvastgoed.nl/nl/Aanbod?filter=house_filter&_token=9N5pjCgPW0vTY1ay5YUTQznN5DSqahC8qwIiXyDY&city=&type=studio&min_price=&max_price=",
                "property_type" : "studio"
            },
            {
                "url" : "https://www.goethvastgoed.nl/nl/Aanbod?filter=house_filter&_token=9N5pjCgPW0vTY1ay5YUTQznN5DSqahC8qwIiXyDY&city=&type=appartment&min_price=&max_price=",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.goethvastgoed.nl/nl/Aanbod?filter=house_filter&_token=9N5pjCgPW0vTY1ay5YUTQznN5DSqahC8qwIiXyDY&city=&type=room&min_price=&max_price=",
                "property_type" : "room"
            },
            {
                "url" : "https://www.goethvastgoed.nl/nl/Aanbod?filter=house_filter&_token=9N5pjCgPW0vTY1ay5YUTQznN5DSqahC8qwIiXyDY&city=&type=villa&min_price=&max_price=",
                "property_type" : "house"
            },
            {
                "url" : "https://www.goethvastgoed.nl/nl/Aanbod?filter=house_filter&_token=9N5pjCgPW0vTY1ay5YUTQznN5DSqahC8qwIiXyDY&city=&type=maisonnette&min_price=&max_price=",
                "property_type" : "apartment"
            },
        ]# LEVEL 1
        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING LEVEL 1
    def parse(self, response):
        for follow_url in response.css("label.direct > a::attr(href)").extract():
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        next_page = response.xpath("//a[.='»']/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')}
            )

    # 2. SCRAPING LEVEL 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Goethvastgoed_PySpider_" + self.country + "_" + self.locale)
        title=response.xpath("//h3[@class='house-title text-center']/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        item_loader.add_value("external_link", response.url) 
        external_id = (response.url).split("/")[-1]
        try:
            external_id = external_id.split("?")[0]
        except:
            pass   
        item_loader.add_value("external_id", external_id)

        desc = "".join(response.xpath("//div[@class='house-description']//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
        
            if "energielabel" in desc.lower():
                try:
                    energy=desc.split("energielabel")[1].strip().split(" ")[0]
                except:
                    energy=False
                if energy:
                    item_loader.add_value("energy_label", energy)
                
        
        latitude = response.xpath("//main[@class='control-main house']/@data-lat").get()
        longitude = response.xpath("//main[@class='control-main house']/@data-lng").get()
        if latitude and longitude:
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
            
        city =  response.xpath("//span[contains(@class,'city')]/text()").get()
        address =  response.xpath("//span[contains(@class,'address')]/text()").get()
        item_loader.add_value("address", address)
        zipcode = address.split(" ")[0] + " " + address.split(" ")[1].strip()
        if zipcode and not zipcode.split(" ")[0].isdigit(): zipcode = zipcode.split(" ")[0]
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("city", city)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        square_meters = response.xpath("//ul[@class='feature-property-list']/li[@class='row']/div[.='Woonoppervlakte']/parent::*/div[2]/text()").get()
        if square_meters and square_meters != "-":
            square_meters = square_meters.strip("m")
            item_loader.add_value("square_meters", square_meters)
        
        room_count = response.xpath("//ul[@class='feature-property-list']/li[@class='row']/div[.='Kamer(s)']/parent::*/div[2]/text()").get()
        if room_count and room_count != "0":    
            item_loader.add_value("room_count", room_count)
        
        available_date = response.xpath("//ul[@class='feature-property-list']/li[@class='row']/div[contains(.,'Beschikbaar per')]/parent::*/div[2]/text()").get()
        if available_date and available_date.isalpha() != True:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)
        
        images = [x for x in response.xpath("//div[@id='sync1']/div/img/@src").getall()]
        if images:
            item_loader.add_value("images", list(set(images)))
            # item_loader.add_value("external_images_count", str(len(images)))
        
        price = response.xpath("//ul[@class='feature-property-list']/li[@class='row']/div[contains(.,'Prijs')]/parent::*/div[2]/text()").get()
        if price and price != "-" and price.strip() != "€": 
            price = price.split(" ")[1]
            item_loader.add_value("rent", price)
        rentcheck=item_loader.get_output_value("rent")
        if not rentcheck:
            return 

        item_loader.add_value("currency", "EUR")
        
        deposit = response.xpath("//ul[@class='feature-property-list']/li[@class='row']/div[contains(.,'Borg')]/parent::*/div[2]/text()").get()
        if deposit and deposit != "-" and deposit.strip() != "€":
            item_loader.add_value("deposit", deposit.split(" ")[1])
        
        furnished = response.xpath("//ul[@class='feature-property-list']/li[@class='row']/div[contains(.,'Gemeubileerd')]/parent::*/div[2]/text()").get()
        if furnished:
            if furnished.lower() == "ja":
                item_loader.add_value("furnished", True)
            else:
                item_loader.add_value("furnished", False)

        parking = response.xpath("//div[.='Parkeergelegenheid']/parent::*/div[2]/i/@class").get()
        if parking:
            if parking == "fa fa-check":
                item_loader.add_value("parking", True)
            else:
                item_loader.add_value("parking", False)

        elevator = response.xpath("//div[.='Lift']/parent::*/div[2]/i/@class").get()
        if elevator:
            if elevator == "fa fa-check":
                item_loader.add_value("elevator", True)
            else:
                item_loader.add_value("elevator", False)
        
        balcony = response.xpath("//div[contains(.,'Balkon')]/parent::*/div[2]/i/@class").get()
        if balcony:
            if balcony == "fa fa-check":
                item_loader.add_value("balcony", True)
                item_loader.add_value("terrace", True)
            else:
                item_loader.add_value("balcony", False)
                item_loader.add_value("terrace", False)
     
        item_loader.add_value("landlord_name", "GOETH VASTGOED EINDHOVEN")
        item_loader.add_value("landlord_phone", "040 213 02 23")
        item_loader.add_value("landlord_email", "info@goethvastgoed.nl")
        
        status=response.xpath("//div/img[@alt='rented']/@alt").get()
        if status:
            return
        else:
            yield item_loader.load_item()

    # 3. PAGINATION LEVEL 1
    def paginate(self, response):
        next_page_url = response.css(
            "ul.pagination a::attr(href)"
        ).extract_first()  # pagination("next button") <a> element here
        if next_page_url is not None:
            return response.follow(next_page_url, self.parse)
