import requests
import scrapy
from scrapy import Request, FormRequest

from ..loaders import ListingLoader

import json

counter = 2
pos = 1
prob = ''


class Ziantoni_PySpider_italy_it(scrapy.Spider):
    name = 'ziantoni_it'
    allowed_domains = ['ziantoni.it']
    country = 'italy'
    locale = 'it'
    execution_type = 'testing'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"

    def start_requests(self):
        start_urls = [
            'https://www.ziantoni.it/re/selezione-immobili-map.aspx?tipoTabella=offerta&categoria=Residenziale+Affitto']
        for url in start_urls:
            yield scrapy.Request(url=url, method="GET", meta={'dont_merge_cookies': True}, callback=self.any)

    def any(self, response):
        for x in range (1,3) :
            cookie = response.headers['Set-Cookie']
            head = {
                'Cookie': cookie,
                "Referer": "https://www.ziantoni.it/re/selezione-immobili-map.aspx?categoria=Vetrina+Affitto",
                'Content-Type': 'application/json; charset=utf-8'}

            body = {"page": f'{x}', "listType": "Lista"}

            yield Request(url="https://www.ziantoni.it/re/selezione-immobili-map.aspx/GetPaged", method='POST',
                          headers=head, body=json.dumps(body), callback=self.parse)

    def parse(self, response,**kwargs):
        l=response.text
        l=l.split('\\\"')
        urls=[]
        for i in l :
            if "scheda.aspx" in i :
                if i not in urls :
                    urls.append(i)
        for x in range(len(urls)):
            url = "https://www.ziantoni.it/re/" + urls[x][0:33]
            yield Request(url=url, callback=self.parse_area)

    def parse_area(self, response):
        global pos
        desc = response.xpath('//*[@id="overview"]/article/div[3]/p/text()').extract()[0].strip()
        if "AFFITTATO" in desc:
            yield
        else:
            item_loader = ListingLoader(response=response)
            item_loader.add_value('external_link', response.url)
            item_loader.add_value('external_source', self.external_source)
            title = str.strip(response.xpath( f'.//h4[@class="title"]//text()').extract()[1]).split("-")[0].replace(".", "")
            item_loader.add_value('title', title)
            item_loader.add_value("description", desc)
            rent = response.xpath('//*[@id="overview"]/article/div[1]/h5/span[2]//text()').extract()[
                0].strip().replace(" €", "").replace(".", "")
            item_loader.add_value('rent', int(rent))
            item_loader.add_value('currency', "EUR")
            proptype = response.xpath('//*[@id="overview"]/article/div[1]/h5/span[2]//text()').extract()[
                1].replace("-\r\n", "").replace(" ", "")
            if proptype == "Attico":
                item_loader.add_value('property_type', "house")
            elif proptype == "Appartamento":
                item_loader.add_value('property_type', "apartment")
            sq = response.xpath('//*[@id="overview"]/article/div[2]/span[1]/text()').extract()[
                0].replace("\r\n", "").replace(" ", "").split("-")[0].replace("mq", "")
            item_loader.add_value("square_meters", int(sq))
            room = int(response.xpath(
                '//*[@id="overview"]/article/div[2]/span[2]/text()').extract()[0].split(" ")[0])
            item_loader.add_value('room_count', room)
            bath = int(response.xpath(
                '//*[@id="overview"]/article/div[2]/span[3]/text()').extract()[0].split(" ")[0])
            item_loader.add_value('bathroom_count', bath)
            park = int(response.xpath(
                '//*[@id="overview"]/article/div[2]/span[4]/text()').extract()[0].split(" ")[0])
            if park:
                item_loader.add_value("parking", True)
            else:
                item_loader.add_value("parking", False)
            imgs = response.xpath(
                './/a[@class="swipebox"]//img//@src').extract()
            item_loader.add_value('images', imgs)
            item_loader.add_value("external_images_count",len(imgs))
            pos += 1
            address = str.strip(response.xpath( f'.//h4[@class="title"]//text()').extract()[1])
            responseGeocode = requests.get(
                f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={address}&maxLocations=1")
            responseGeocodeData = responseGeocode.json()
            longitude = responseGeocodeData['locations'][0]['feature']['geometry']['x']
            latitude = responseGeocodeData['locations'][0]['feature']['geometry']['y']
            responseGeocode = requests.get(
                f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
            responseGeocodeData = responseGeocode.json()
            zipcode = responseGeocodeData['address']['Postal']
            city = responseGeocodeData['address']['City']
            # address = responseGeocodeData['address']['Match_addr']
            longitude = str(longitude)
            latitude = str(latitude)
            item_loader.add_value("zipcode", zipcode)
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("city", city)
            item_loader.add_value("address", address)
            item_loader.add_value("position", pos)
            id = response.xpath('.//h1[@class="page-title"]//text()').extract()[0].replace("Dettagli\r\n                    ", "")

            item_loader.add_value("external_id", id)
            item_loader.add_value("landlord_name", "Agenzia Immobiliare Ziantoni")
            item_loader.add_value("landlord_phone", "063227000")
            item_loader.add_value("landlord_email", "info@ziantoni.it")
            response.xpath('.//*[@id="paginator_top"]/ul/li[2]/a').extract()
            yield item_loader.load_item()