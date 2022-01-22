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
from datetime import datetime 


class MySpider(Spider):
    name = 'homeinvestbelgium_be'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Homeinvestbelgium_PySpider_france"

    custom_settings = {
        "CONCURRENT_REQUESTS" : 4,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "HTTPCACHE_ENABLED":False,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 3,
        "PROXY_US_ON" : True
    }
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.immoweb.be/en/search-results/apartment/for-rent?countries=ALL&customerIds=1356378&page=1&orderBy=newest",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.immoweb.be/en/search-results/house/for-rent?countries=ALL&customerIds=1356378&page=1&orderBy=newest",
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
        data = json.loads(response.body)
        page = response.meta.get("page", 2)

        seen = False
        
        for item in data["results"]:
            follow_url = f"https://www.immoweb.be/fr/annonce/appartement/a-louer/etterbeek/1040/" + str(item["id"])
            external_id = item["id"]
            room_count = item["property"]["bedroomCount"]
            city = item["property"]["location"]["province"]
            zipcode = item["property"]["location"]["postalCode"]
            address = ''
            if item["property"]["location"]["number"]:
                address += item["property"]["location"]["number"] + " "
            if item["property"]["location"]["street"]:
                address += item["property"]["location"]["street"] + " "
            if item["property"]["location"]["province"]:
                address += item["property"]["location"]["province"] + " "
            if item["property"]["location"]["country"]:
                address += item["property"]["location"]["country"] + " "
            if item["property"]["location"]["postalCode"]:
                address += item["property"]["location"]["postalCode"] + " "
            address = address.strip()
            floor = item["property"]["location"]["floor"]
            latitude = item["property"]["location"]["latitude"]
            longitude = item["property"]["location"]["longitude"]
            title = item["property"]["title"]
            
            if item["transaction"]["rental"]:
                rent = item["transaction"]["rental"]["monthlyRentalPrice"]
                utilities = item["transaction"]["rental"]["monthlyRentalCosts"]
            else:
                rent = ""
                utilities = ""
          
            seen = True
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type'), 'external_id': external_id,
                            'room_count': room_count, 'city': city, 'zipcode': zipcode, 'address': address, 'floor': floor,
                            'latitude': latitude, 'longitude': longitude, 'title': title, 'rent': rent, 'utilities': utilities})
        
        # next_page = response.xpath("//a[contains(@class,'pagination__link--next button')]/@href").get()
        # if next_page:
        #     yield Request(response.urljoin(next_page), callback=self.parse, meta={'property_type': response.meta.get('property_type')})
        if response.meta.get('property_type') =="house":
            if page == 2 or seen:
                url = f"https://www.immoweb.be/en/search-results/house/for-rent?countries=BE&page={page}&orderBy=relevance"
                yield Request(
                    url=url,
                    callback=self.parse,
                    meta={'property_type': response.meta.get('property_type'), "page":page+1}
                )
        else:
            if page == 2 or seen:
                url = f"https://www.immoweb.be/fr/search-results/appartement/a-louer?countries=BE&page={page}&orderBy=relevance"
                yield Request(
                    url=url,
                    callback=self.parse,
                    meta={'property_type': response.meta.get('property_type'), "page":page+1}
                )
            
           
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        if response.xpath("//div[@id='main-container']//h1/text()[contains(.,'Studio') or contains(.,'studio')]").get(): item_loader.add_value("property_type", 'studio')
        else: item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
      
        external_id = response.meta.get('external_id')
        if external_id:
            item_loader.add_value("external_id", str(external_id).strip())
        room = "".join(response.xpath("//span[@class='overview__text']/text()[contains(.,'chambre')]").extract())        
        if room:
            item_loader.add_value("room_count", room.split("chambre")[0].split("-")[-1].strip())
        else: 
            room = "".join(response.xpath("//h1[@class='classified__title']/text()").extract())
            if "studio" in room.lower():
                item_loader.add_value("room_count","1")
            else:return

        city = response.meta.get('city')
        if city:
            item_loader.add_value("city", city.strip())

        zipcode = response.meta.get('zipcode')
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())

        address = response.meta.get('address')
        if address:
            item_loader.add_value("address", address.strip())

        floor = response.meta.get('floor')
        if floor:
            item_loader.add_value("floor", str(floor).strip())

        latitude = response.meta.get('latitude')
        if latitude:
            item_loader.add_value("latitude", str(latitude).strip())

        longitude = response.meta.get('longitude')
        if longitude:
            item_loader.add_value("longitude", str(longitude).strip())
        
        if not item_loader.get_collected_values("latitude") or not item_loader.get_collected_values("longitude"):
            latitude = response.xpath("//script[contains(.,'latitude')]/text()").get()
            if latitude:
                item_loader.add_value("latitude", latitude.split('"latitude":')[1].split(',')[0].strip())
                item_loader.add_value("longitude", latitude.split('"longitude":')[1].split(',')[0].strip())

        title = response.xpath("//title/text()").get()
        if title: item_loader.add_value("title", title.strip())
        
        rent = response.meta.get('rent')
        if rent:
            item_loader.add_value("rent", str(rent).strip())
            item_loader.add_value("currency", 'EUR')
        rentcheck=item_loader.get_output_value("rent")
        if not rentcheck:
            if rent:
                rent=rent.split("-")[0].replace("\xa0€")
                
                item_loader.add_value("rent",rent)

        utilities = response.meta.get('utilities')
        if utilities:
            item_loader.add_value("utilities", str(utilities).strip())

        script_content = response.xpath("//script[contains(.,'window.classified')]/text()").get()
        if script_content:
            data = json.loads(script_content.split('window.classified =')[1].strip().strip(';'))

            square_meters = data["property"]["netHabitableSurface"]
            if square_meters:
                item_loader.add_value("square_meters", str(int(float(square_meters))).strip())
            squcheck=item_loader.get_output_value("square_meters")
            if not squcheck:
                squ=response.xpath("//section[@class='classified__section']/div/div//div[contains(.,'de terrain')]/div/span/text()").get()
                if squ:
                    squ=re.findall("\d+",squ)
                item_loader.add_value("square_meters",squ)
           
            bathroom_count = data["property"]["bathroomCount"]
            if bathroom_count:
                item_loader.add_value("bathroom_count", str(bathroom_count).strip())
            else:
                bathroom_count = response.xpath("//tr[th[.='Salles de douche']]/td/text() | //tr[th[.='Toilettes']]/td/text()").extract_first()
                if bathroom_count:
                    item_loader.add_value("bathroom_count", str(bathroom_count).strip())

            available_date = data["transaction"]["availabilityDate"]
            if available_date:
                available_date = available_date.split('T')[0].strip()
                date_parsed = dateparser.parse(available_date, date_formats=["%d %B %Y"], languages=['fr'])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    if date_parsed.strftime("%Y") < str(datetime.now().year):
                        return
                    else:
                        item_loader.add_value("available_date", date2)

            description = data["property"]["description"]
            if description:
                item_loader.add_value("description", description.replace('\xa0', '').strip())
            else:
                desc = " ".join(response.xpath("//div[@id='classified-description-content-text']//text()").getall())
                if desc:
                    item_loader.add_value("description", desc.strip())
            images = [x["largeUrl"] for x in data["media"]["pictures"]]
            if images:
                item_loader.add_value("images", images)
                item_loader.add_value("external_images_count", len(images))

            if "certificates" in data:
                energy_label = data["transaction"]["certificates"]["epcScore"]
                if energy_label:
                    item_loader.add_value("energy_label", energy_label.strip())
           
            if  data["transaction"].get("rental"):
                pets_allowed = data["transaction"]["rental"].get("areBigPetsAllowed")
                pets_allowed2 = data["transaction"]["rental"].get("areSmallPetsAllowed")

            if pets_allowed == True or pets_allowed2 == True:
                item_loader.add_value("pets_allowed", True)


            parking = data["property"]["parkingCountIndoor"]
            parking2 = data["property"]["parkingCountOutdoor"]
            if parking:
                if parking > 0:
                    item_loader.add_value("parking", True)
            elif parking2:
                if parking2 > 0:
                    item_loader.add_value("parking", True)
            
            balcony = data["property"]["hasBalcony"]
            if balcony:
                item_loader.add_value("balcony", balcony)

            furnished = data["transaction"]["rental"]["isFurnished"]
            if furnished:
                item_loader.add_value("furnished", furnished)
            
            elevator = data["property"]["hasLift"]
            if elevator:
                item_loader.add_value("elevator", elevator)

            terrace = data["property"]["hasTerrace"]
            if terrace:
                item_loader.add_value("terrace", terrace)
            
            swimming_pool = data["property"]["hasSwimmingPool"]
            if swimming_pool:
                item_loader.add_value("swimming_pool", swimming_pool)

            if data["property"]["kitchen"]:
                dishwasher = data["property"]["kitchen"]["hasDishwasher"]
                if dishwasher:
                    item_loader.add_value("dishwasher", dishwasher)
                
                washing_machine = data["property"]["kitchen"]["hasWashingMachine"]
                if washing_machine:
                    item_loader.add_value("washing_machine", washing_machine)
            
            landlord_name = data["customers"][0]["name"]
            if landlord_name:
                item_loader.add_value("landlord_name", landlord_name.strip())
            
            landlord_phone = data["customers"][0]["phoneNumber"]
            if landlord_phone:
                item_loader.add_value("landlord_phone", landlord_phone.strip())
            phonecheck=item_loader.get_output_value("landlord_phone")
            if not phonecheck:
                item_loader.add_value("landlord_phone","+32.2.333.25.05")
            
            landlord_email = data["customers"][0]["email"]
            if landlord_email:
                item_loader.add_value("landlord_email", landlord_email.strip())

            if not item_loader.get_collected_values("landlord_name"):
                item_loader.add_value("landlord_name", "Home Invest Belgium")
            
        yield item_loader.load_item()