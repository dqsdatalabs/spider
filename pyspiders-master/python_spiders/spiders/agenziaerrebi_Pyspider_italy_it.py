import requests
import scrapy
from scrapy import Spider, Request, FormRequest
from ..loaders import ListingLoader
import json

counter = 2
pos = 1
prob = ''


class agenziaerrebi_Pyspider_italy_it(scrapy.Spider):
    name = 'agenziaerrebi'
    allowed_domains = ['agenziaerrebi.it']
    country = 'italy'
    locale = 'it'
    execution_type = 'testing'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"

    def start_requests(self):
        start_urls = ['https://www.agenziaerrebi.it/immobili.php']
        for url in start_urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response, **kwargs):
        global counter
        urls = response.xpath('.//div[@class="actions row between-xs middle-xs"]//a//@href').extract()
        for x in range(len(urls)):
            url = "https://www.agenziaerrebi.it/" + urls[x]
            yield Request(url=url, callback=self.parse_area)
        count =len(response.xpath('/html/body/main/div[2]/div/div/div[2]/div/div[12]/div/ul/text()').extract())
        for x in range(2,count-2) :
            yield FormRequest("https://www.agenziaerrebi.it/immobili.php", formdata={'p': f'{x}'}, callback=self.parse)

    def parse_area(self, response):
        global pos
        cont = response.xpath('/html/body/main/div[1]/div/div/div[1]/div[4]/div/div[2]/div/span/text()').extract()[0]
        if cont == 'Affitto' :
            im = response.xpath('/html/body/main/div[1]/div/div/div[1]/div[3]/div[1]/div[2]/div/div/img/@data-src').extract()
            imgs = []
            for i in im :
                x="https://www.agenziaerrebi.it"+i
                imgs.append(x)
            all=response.xpath('.//div[@class="mdc-card p-3 mt-3"]//text()').extract()
            ind=all.index('Descrizione')
            desc=str.strip(all[ind+1])
            item_loader = ListingLoader(response=response)
            item_loader.add_value('external_link', response.url)
            item_loader.add_value('external_source', self.external_source)
            title = str.strip(response.xpath(f'.//h2[@class="uppercase"]//text()').extract()[0])
            city = str.strip(response.xpath(f'/html/body/main/div[1]/div/div/div[1]/div[4]/div/div[3]/span[2]//text()').extract()[0])
            ref = str.strip(response.xpath(f'/html/body/main/div[1]/div/div/div[1]/div[2]/div[2]/span/strong/text()').extract()[0])
            landname = str.strip(response.xpath(f'/html/body/main/div[1]/div/div/aside/div/div[1]/div/div/p[2]/span/text()').extract()[0])
            landphone = str.strip(response.xpath(f'/html/body/main/div[1]/div/div/aside/div/div[1]/div/div/p[3]/span/text()').extract()[0])
            item_loader.add_value('title', title)
            item_loader.add_value('city', city)
            item_loader.add_value('images', imgs)
            item_loader.add_value('external_images_count', len(imgs))
            item_loader.add_value("description", desc)
            rent = response.xpath('/html/body/main/div[1]/div/div/div[1]/div[2]/div[2]/h2//text()').extract()[1].replace("€ ", "").replace(".", "")
            item_loader.add_value('rent', int(rent))
            item_loader.add_value('currency', "EUR")
            if "Bilocale" in title or "Appartamento" in title:
                item_loader.add_value('property_type', "apartment")
            elif "Attico" in title:
                item_loader.add_value('property_type', "house")
            sq = response.xpath('/html/body/main/div[1]/div/div/div[1]/div[4]/div/div[9]//span[2]//text()').extract()[0]
            item_loader.add_value("square_meters", int(sq))
            room = int(
                response.xpath('/html/body/main/div[1]/div/div/div[1]/div[4]/div/div[8]//span[2]//text()').extract()[0])
            item_loader.add_value('room_count', room)
            bath = int(
                response.xpath('/html/body/main/div[1]/div/div/div[1]/div[4]/div/div[7]//span[2]//text()').extract()[0])
            item_loader.add_value('bathroom_count', bath)
            park = response.xpath('.//div[@class="col-xs-12 col-sm-4 row middle-xs"]//text()').extract()
            park = [x.strip() for x in park if x]
            park = [x for x in park if x]
            if 'Balconi' in park:
                item_loader.add_value("balcony", True)
            if 'Posti auto scoperti' in park:
                item_loader.add_value("parking", True)
            if 'Terrazzo' in park:
                item_loader.add_value("terrace", True)

            pos += 1
            address = str.strip(response.xpath('.//span[@class="fw-500 text-muted"]//text()').extract()[0])
            item_loader.add_value("address", address)
            responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={address}&maxLocations=1")
            responseGeocodeData = responseGeocode.json()
            longitude = responseGeocodeData['locations'][0]['feature']['geometry']['x']
            latitude = responseGeocodeData['locations'][0]['feature']['geometry']['y']
            responseGeocode = requests.get( f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
            responseGeocodeData = responseGeocode.json()
            zipcode = responseGeocodeData['address']['Postal']
            address = responseGeocodeData['address']['Match_addr']
            longitude = str(longitude)
            latitude = str(latitude)
            item_loader.add_value("zipcode",zipcode)
            item_loader.add_value("longitude",longitude)
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("position", pos)
            item_loader.add_value("external_id", ref)
            name=str.strip(response.xpath(f'/html/body/main/div[1]/div/div/aside/div/div[1]/div/div/h2/text()').extract()[0])
            item_loader.add_value("landlord_name",name)
            item_loader.add_value("landlord_phone", landphone)
            item_loader.add_value("landlord_email", landname)
            yield item_loader.load_item()
        else :
            yield