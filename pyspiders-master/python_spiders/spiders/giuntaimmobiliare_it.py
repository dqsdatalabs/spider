# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class GiuntaImmobiliareSpider(Spider):
    name = 'Giuntaimmobiliare_it'
    country='italy'
    locale='it' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.giuntaimmobiliare.it"]
    start_urls = ["https://giuntaimmobiliare.it/immobili/?TipoTrattativa=1&CodiceLocalita=&Localita=&IDImmobiliTipologia=1&ImportoMinimo=0&ImportoMassimo=40"]

    def parse(self, response):
        for url in response.css("li.clearfix div.text h3 a::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type = "apartment"
        rent = response.css("div.details span.importo::text").get()
        currency = "EUR"
        title = response.css("div.text h2::text").get()
        room_count = response.css("div.details span.vani::text").get().split(",")[0]
        square_meters = response.css("div.details span.metri-quadri::text").get()
        energy_label = response.css("div.details span.classe-energetica::text").get()

        images = response.css("ul.bxslider li a img::attr(src)").getall()
        images_to_add = []
        for image in images:
            images_to_add.append(response.urljoin(image))

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("title", title)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("images", images_to_add)
       
        yield item_loader.load_item()



