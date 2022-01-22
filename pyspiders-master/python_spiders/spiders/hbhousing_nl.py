# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json


class MySpider(Spider):
    name = 'hbhousing_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    
    def start_requests(self):
        headers = {
            "content-type": "application/json",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36",
            "origin": "https://www.hbhousing.nl"
        }

        start_urls = [
            {
                "r_query" : "(nlKind:appartement)",
                "property_type" : "apartment"
            },
            {
                "r_query" : "(nlKind:hoekwoning OR nlKind:vrijstaandhuis OR nlKind:twee-onder-1-kap)",
                "property_type" : "house"
            },
            {
                "r_query" : "(nlKind:studio)",
                "property_type" : "studio"
            },

        ] #LEVEL 1
        
        for item in start_urls:
            r_query = item.get("r_query")
            payload = {
                "q": f"for_rent:true AND leased:false AND {r_query}",
                "limit": 200,
                "sort": "id"
            }
       
            url = "https://70198239-e7fa-404f-b5c0-a1b54897b97d-bluemix.cloudant.com/spe-hbhousing/_design/objects/_search/search"
            yield Request(url, self.parse, method="POST", headers=headers, body=json.dumps(payload), meta={'property_type': item.get('property_type')})
    
    # 1. FOLLOWING
    def parse(self, response):
        
        data = json.loads(response.body)
    
        for item in data["rows"]:
            follow_url = "https://www.hbhousing.nl/huurwoning/" + item["fields"]["url"]
            lat = item["fields"]["latitude"]
            lon = item["fields"]["longitude"]
            yield Request(follow_url, callback=self.populate_item, meta={"lat": lat, "lon": lon, 'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Hbhousing_PySpider_" + self.country + "_" + self.locale)
        
        lat = response.meta.get("lat")
        lon = response.meta.get("lon")
        title = " ".join(response.xpath("//h1/span[@class='orange']/text()").extract())
        item_loader.add_value("title", title)
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("property_type", response.meta.get('property_type'))

        desc = "".join(response.xpath("//div[@id='description']//div[@class='row']//div/text()").extract())
        item_loader.add_value("description", desc.strip())

        # price = "".join(response.xpath("//div[@class='container']//ul/li[.//div[.='Huurprijs']]//div[contains(.,'€')]/text()").extract())
        # if price:
        #     item_loader.add_value("rent", price.strip().split("€")[1].strip())
        #     item_loader.add_value("currency", "EUR")

        external_id = response.url
        if external_id:
            if external_id.strip().endswith('/'):
                external_id = external_id.split('/')[-2].strip()
            else:
                external_id = external_id.split('/')[-1].strip()
            item_loader.add_value("external_id", external_id)

        if desc:
            if 'badkamer' in desc.lower():
                bathroom_count = desc.lower().split('badkamer')[0].strip().split(' ')[-1].strip()
                if bathroom_count.isnumeric():
                    item_loader.add_value("bathroom_count", bathroom_count)
            if 'energielabel' in desc.lower():
                item_loader.add_value("energy_label", energy_label.split('energielabel')[-1].split('.')[0].strip())
            if 'parkeren' in desc.lower():
                item_loader.add_value("parking", True)
            if 'balkon' in desc.lower():
                item_loader.add_value("balcony", True)
            if 'lift' in desc.lower():
                item_loader.add_value("elevator", True)
            if 'terras' in desc.lower():
                item_loader.add_value("terrace", True)
            if 'zwembad' in desc.lower():
                item_loader.add_value("swimming_pool", True)
            if 'vaatwasser' in desc.lower():
                item_loader.add_value("dishwasher", True)
            if 'wasmachine' in desc.lower():
                item_loader.add_value("washing_machine", True)
        
        floor_plan_images = [x.split('url(')[-1].split(')')[0].strip() for x in response.xpath("//div[contains(@style,'plattegrond')]/@style").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        water_cost = response.xpath("//br[contains(following-sibling::text(),'Water')]/following-sibling::text()[1]").get()
        if water_cost:
            water_cost = water_cost.split('euro')[-1].split(',')[0].strip()
            item_loader.add_value("water_cost", water_cost)
        

       
        square = response.xpath(
            "//div[@class='col-sm-6 intro-text']/ul/li[.//div[.='Oppervlakte']]//div[2]/text()").get()
        if square:
            item_loader.add_value(
                "square_meters", square.split("m²")[0].strip()
            )
        room_count = response.xpath(
            "//div[@class='col-sm-6 intro-text']/ul/li[.//div[.='Slaapkamers']]//div[2]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)

        address = "".join(response.xpath("//h1/span[@class='orange']/text()").extract())
        if address:
            item_loader.add_value("address", address)
            # if "," in address:
            #     item_loader.add_value("city", address.split(",")[1].strip())
            
        available_date = response.xpath(
            "//div[@class='col-sm-6 intro-text']/ul/li[.//div[.='Beschikbaar per']]//div[2]/text()[.!='DIRECT']").get()
        if available_date:
            item_loader.add_value("available_date", available_date)
 

        furnished = response.xpath(
            "//div[@class='col-sm-6 intro-text']/ul/li[.//div[.='Oplevering']]//div[2]/text()[.='Gemeubileerd']"
        ).get()
        if furnished:
            item_loader.add_value("furnished", True)
 
        pet = response.xpath(
            "//div[@class='col-sm-6 intro-text']/ul/li[.//div[.='Huisdieren']]//div[2]/text()").get()
        if pet:
            if "nee" in pet:
                item_loader.add_value("pets_allowed", False)
            else:
                item_loader.add_value("pets_allowed", True)

        images1= response.xpath("//div[@class='ps2-image-thumb ng-star-inserted']/div/@style").extract()
        images = []
        for i in images1:
            img = i.split('background-image:url(')[1].split(');')[0]
            images.append(img)
        images2 = [
            response.urljoin(x)
            for x in images
        ]
        if images:
            item_loader.add_value("images", images2)
            item_loader.add_value("external_images_count", len(images2))
        
        
        item_loader.add_value("landlord_phone", "+31 (0)20 617 03 79")
        item_loader.add_value("landlord_name", "Hbhousing")
        item_loader.add_value("landlord_email", "info@hbhousing.nl")

       
        item_loader.add_value("latitude", str(lat))
        item_loader.add_value("longitude", str(lon))

        rent_url = "https://70198239-e7fa-404f-b5c0-a1b54897b97d-bluemix.cloudant.com/spe-hbhousing/_design/objects/_search/cleanurl"
        ref_url = response.url.split('hbhousing.nl/huurwoning/')[1].strip()
        payload="{\"q\": \"cleanurl:\\\"" + ref_url + "\\\"\",\"limit\": 1}"
        headers = {
            'authority': '70198239-e7fa-404f-b5c0-a1b54897b97d-bluemix.cloudant.com',
            'accept': 'application/json, text/plain, */*',
            'content-type': 'application/json',
            'origin': 'https://www.hbhousing.nl',
            'sec-fetch-site': 'cross-site',
            'sec-fetch-mode': 'cors',
            'sec-fetch-dest': 'empty',
            'referer': response.url,
            'accept-language': 'tr,en;q=0.9'
        }
        yield Request(rent_url, method="POST", headers=headers, body=payload, callback=self.get_rent, meta={"item_loader": item_loader})

    def get_rent(self, response):

        item_loader = response.meta.get("item_loader")

        data = json.loads(response.body)
        if 'rows' in data.keys():
            if len(data["rows"]) > 0:
                item = data["rows"][0]
                if 'fields' in item.keys():                   
                    if 'rentPrice' in item["fields"].keys():
                        rent = int(item["fields"]["rentPrice"])
                        item_loader.add_value("rent", str(rent))
                        item_loader.add_value("currency", "EUR")

                    if 'city' in item["fields"].keys():
                        item_loader.add_value("city", item["fields"]["city"].strip())

                    if 'postalcode' in item["fields"].keys():
                        item_loader.add_value("zipcode", item["fields"]["postalcode"].strip())

        yield item_loader.load_item()