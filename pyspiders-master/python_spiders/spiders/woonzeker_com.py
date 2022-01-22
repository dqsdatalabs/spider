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
from geopy.geocoders import Nominatim


class MySpider(Spider):
    name = 'woonzeker_com'
    start_urls = ['https://woonzeker.com/Te-huur/huurwoning-te-huur-in-den-haag/0-rooms/0-persons/0']  # LEVEL 1
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        for item in response.xpath("//ul[contains(@class,'building-list row')]/li"):
            follow_url = response.urljoin(item.xpath("./@data-property-link").extract_first())
            lat_lng = item.xpath("./@data-property-lnglat").extract_first()
            square_meters = item.xpath("normalize-space(.//span[@class='surface']/text()[contains(.,'m')])").get()
            if lat_lng:
                lat = lat_lng.split(",")[1]
                lng = lat_lng.split(",")[0]
                yield Request(follow_url, callback=self.populate_item,meta={"lat": lat, "lng": lng, "square" : square_meters})
      
            else:
                yield Request(follow_url, callback=self.populate_item,meta={ "square" : square_meters})
      
        
        if page < int(response.xpath("//ul[contains(@class,'pagination')]/li[last()]/a/text()").get().strip()):
            url = f"https://woonzeker.com/Te-huur/huurwoning-te-huur-in-den-haag/{page}/0-rooms/0-persons"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        status = response.xpath("//div[contains(@style,'color: #ff9c09')]/text()").get()
        if status and "verhuurd" in status.lower().strip():
            return

        item_loader.add_value("external_source", "Woonzeker_PySpider_" + self.country + "_" + self.locale)
        address = ""
        if response.meta.get("lat") and response.meta.get("lng"):
            lat = response.meta.get("lat")
            lng = response.meta.get("lng")
            item_loader.add_value("latitude", lat)
            item_loader.add_value("longitude", lng)
            geolocator = Nominatim(user_agent = response.url)
            locationLatLng = lat + ", " + lng
            try:
                location = geolocator.reverse(locationLatLng, timeout=None)
                if location.address:
                    address = location.address
                    if location.raw['address']['city']:
                        city = location.raw['address']['city']
                        if location.raw['address']['postcode']:
                            zipcode = location.raw['address']['postcode']
            except:
                address = None
                zipcode = None
                city = None

            if address:
                item_loader.add_value("address", address)
                item_loader.add_value("zipcode", zipcode)
                item_loader.add_value("city", city)

        if not address:
            address =",".join(response.xpath("//div[@class='detail-description']//h1//text() | //div[@class='detail-description']//h2//text()").getall())
            item_loader.add_value("address", address)

        item_loader.add_css("title", "h1")

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", (response.url).strip("/").split("/")[-2])

        
        description ="".join(response.xpath("//div[@class='detail-description']/text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        property_type = response.xpath("//span[text()='Type']/b/text()").get()
        if property_type and ("Dubbel bovenhuis" == property_type or "Appartement" == property_type):
            item_loader.add_value("property_type", "apartment")
        elif property_type and "Etage" in property_type:
            item_loader.add_value("property_type", "house")
        elif property_type and "Studio" in property_type:
            item_loader.add_value("property_type", "studio")
        else:
            return

        square_meters = response.meta.get("square")
        if square_meters:
            square_meters = square_meters.strip("m")
        item_loader.add_value("square_meters", square_meters)
        

        room_count = response.xpath("//span[text()='Slaapkamers']/parent::*/text()").get()
        item_loader.add_value("room_count", room_count)
        
        available_date = response.xpath("normalize-space(//span[text()='Ingangsdatum']/parent::*/text()[2])").get()
        if available_date and available_date.replace(" ","").replace("-","").replace("/","").isalpha() != True:
            date_parsed = dateparser.parse(available_date, date_formats=["%d-%B-%Y"])
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)
        

        
        images = [x for x in response.xpath("//div[@class='property-photoraster']//a/@href[.!='#']").extract()]
        if images:
                item_loader.add_value("images", images)
        

        price = response.xpath("//span[text()='Huurprijs']/b/text()").get()
        if price:
            price = price.split(",")[0].strip("€").strip()

        item_loader.add_value("rent", price)
        item_loader.add_value("currency", "EUR")


        energy_label = response.xpath("//span[text()='Energielabel']/parent::*/div/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)

        

        furnished = response.xpath("//span[text()='Oplevering']/parent::*/div/text()").get()
        if furnished:
            if furnished.lower() == "gestoffeerd":
                item_loader.add_value("furnished", True)
            else:
                item_loader.add_value("furnished", False)
        

        item_loader.add_value("landlord_name", "Woonzeker")
        item_loader.add_value("landlord_phone", "070 - 3 606 202")
        item_loader.add_value("landlord_email", "info@woonzeker.com")

        # yield item_loader.load_item()