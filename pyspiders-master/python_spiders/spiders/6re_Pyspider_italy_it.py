import requests
import scrapy
from scrapy import Spider, Request, FormRequest
from ..loaders import ListingLoader
import json

counter = 2
pos = 1
prob = ''


class _6re_PySpider_italy_it(scrapy.Spider):
    name = '6re'
    allowed_domains = ['6re.it']
    country = 'italy'
    locale = 'it'
    execution_type = 'testing'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"

    def start_requests(self):
        start_urls = ['https://www.6re.it/web/immobili.asp']
        for url in start_urls:
            yield scrapy.FormRequest(url=url, callback=self.parse,
                                     formdata={'tipo_contratto': "A", "cod_categoria": "R"})

    def parse(self, response, **kwargs):
        urls = response.xpath('//a[@class="item-block"]//@href').extract()
        for x in range(len(urls)):
            url = "https://www.6re.it" + urls[x]
            yield Request(url=url, callback=self.parse_area)
        for x in range(2, 4):
            yield scrapy.FormRequest(url='https://www.6re.it/web/immobili.asp', callback=self.parse,
                                     formdata={'num_page': f"{x}", "tipo_contratto": "A", "cod_categoria": "R"})

    def parse_area(self, response):
        global pos
        desc = "".join(response.xpath('.//div[@class="imm-det-des"]//text()').extract())
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.url)
        item_loader.add_value('external_source', self.external_source)
        title = response.xpath('.//*[@id="subheader"]//div[@class="span8"]//h1//text()').extract()[0]
        city = response.xpath('//*[@id="det_prov"]/span/text()').extract()[0]
        item_loader.add_value('title', title)
        item_loader.add_value('city', city)
        item_loader.add_value("description", desc)
        rent = response.xpath('//*[@id="sidebar"]/span[2]//text()').extract()[0].strip().replace("€ ", "").replace(".","")
        item_loader.add_value('rent', int(rent))
        item_loader.add_value('currency', "EUR")
        proptype = response.xpath('//*[@id="li_tipologia"]/strong/text()').extract()[0].replace("-\r\n", "").replace(
            " ", "")
        if proptype == "Attico":
            item_loader.add_value('property_type', "house")
        elif proptype == "Appartamento":
            item_loader.add_value('property_type', "apartment")
        elif proptype == "Bilocale":
            item_loader.add_value('property_type', "apartment")
        elif "Stanza" in proptype:
            item_loader.add_value('property_type', "apartment")
        sq = response.xpath('//*[@id="li_superficie"]/strong/text()').extract()[0].replace("\r\n", '').replace(" ","").split("-")[0].replace("mq", "")
        item_loader.add_value("square_meters", int(sq))
        room = int(response.xpath('//*[@id="li_camere"]/strong/text()').extract()[0].split(" ")[0])
        item_loader.add_value('room_count', room)
        bath = int(response.xpath('//*[@id="li_bagni"]/strong/text()').extract()[0].split(" ")[0])
        item_loader.add_value('bathroom_count', bath)
        park = response.xpath('//*[@id="det_parcheggio"]/strong/text()').extract()
        Eclass = response.xpath('//*[@id="li_clen"]/text()').extract()
        furn = response.xpath('.//div[@class="etichetta accessorio"]//text()').extract()
        elevator = response.xpath('//*[@id="det_ascensore"]/text()').extract()
        f=response.xpath('//*[@id="det_piano"]/span/text()').extract()
        if f :
            item_loader.add_value("floor", f[0].replace(": ", ""))
        if "Arredato" in furn:
            item_loader.add_value("furnished", True)
        if park:
            item_loader.add_value("parking", True)
        if Eclass:
            item_loader.add_value("energy_label", Eclass[0].replace(": ", ""))
        if elevator :
            item_loader.add_value("elevator", True)
        imgs = response.xpath('.//div[@class="watermark"]//img//@src').extract()
        item_loader.add_value('images', imgs)
        item_loader.add_value('external_images_count', len(imgs))
        floor = response.xpath('//*[@id="plangallery"]/li/a/div/img/@src').extract()
        item_loader.add_value('floor_plan_images', floor)
        idd = response.xpath('//*[@id="slide_foto"]/div[2]/strong/text()').extract()
        if idd:
            item_loader.add_value("external_id", idd[0])
        pos += 1
        address = "".join(response.xpath('//*[@id="slide_foto"]/div[4]/span//text()').extract())
        responseGeocode = requests.get(
            f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={city+address}&maxLocations=1")
        responseGeocodeData = responseGeocode.json()
        longitude = responseGeocodeData['locations'][0]['feature']['geometry']['x']
        latitude = responseGeocodeData['locations'][0]['feature']['geometry']['y']
        responseGeocode = requests.get(
            f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        zipcode = responseGeocodeData['address']['Postal']
        city = responseGeocodeData['address']['City']
        address = responseGeocodeData['address']['Match_addr']
        longitude = str(longitude)
        latitude = str(latitude)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("address", city + address)
        item_loader.add_value("position", pos)
        item_loader.add_value("landlord_phone", "0577236036")
        item_loader.add_value("landlord_name", "FIORA MARZOCCHI SERVIZI IMMOBILIARI")
        balcone = response.xpath('//*[@id="det_balcone"]/strong/text()').extract()
        if balcone:
            item_loader.add_value("balcony", True)
        yield item_loader.load_item()
