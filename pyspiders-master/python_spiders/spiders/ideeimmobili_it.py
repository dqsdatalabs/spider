import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader
from scrapy import FormRequest


class IdeeimmobiliItSpider(scrapy.Spider):
    name = 'ideeimmobili_it'
    allowed_domains = ['ideeimmobili.it']
    start_urls = [
        'https://www.ideeimmobili.it/web/immobili.asp?tipo_contratto=A&language=ita&pagref=41577']
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    def parse(self, response):
        for appartment in response.css("#listing > div"):
            url = "https://www.ideeimmobili.it" + appartment.css(
                "article > a").attrib['href']
            yield Request(url,
                          callback=self.populate_item,
                          )

        next_page = response.xpath(
            "//link[@rel='next']").get()

        if next_page is not None:
            global formdata
            
            formdata = {
                "num_page": 1,
            }

            formdata['num_page'] = formdata['num_page'] + 1

            yield FormRequest(
                url="https://www.ideeimmobili.it/web/immobili.asp",
                callback=self.parse,
                formdata=formdata,
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        id = response.css(
            "#slide_foto > div.sfondo_colore3.colore1.right.padder > strong::text").get()

        title = response.css(
            '#subheader > div > div > div.span8 > h1::text').get()

        rent = response.css(
            '#sidebar > span.price.colore1.right::text').get().split("€")[1].strip()
        try:
            if "." in rent:
                rent = rent.split(".")
                rent = rent[0]+rent[1]
        except:
            rent = rent

        description = response.css("div.imm-det-des::text").get()

        images = response.css(
            'li > a.imgw::attr(data-img)').extract()

        space = response.css('#li_superficie > strong::text').get()

        rooms = response.css('#li_vani > strong::text').get()
        bathrooms = response.css('#li_bagni > strong::text').get()

        energy = response.css('#li_clen::text').get().split(" ")[-1]

        address = response.css('#det_zona > span::text').get()
        city = response.css('#det_prov > span::text').get()

        floor = response.css('#det_piano > span::text').get()

        utils = response.css('#det_spese > span::text').get().split(" ")[-1]

        furnished = None
        elevator = None
        if response.css('#det_arredato'):
            furnished = True

        if response.css('#det_ascensore'):
            elevator = True

        # coords = response.xpath(
        #     '//*[@id="all_wrapper"]/div/script[18]/text()').get()
        # lat = coords.split('general_latitude":"')[1].split('",')[0]
        # lng = coords.split('longitude":"')[1].split('",')[0]

        # MetaData
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", id)
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)

        # Property Details
        item_loader.add_value("property_type", "apartment")
        item_loader.add_value("square_meters", int(int(int(space))*10.764))
        item_loader.add_value("room_count", rooms)
        item_loader.add_value("bathroom_count", bathrooms)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("floor", floor)
        # item_loader.add_value("zipcode", zipcode.strip())
        # item_loader.add_value("available_date", avaialble_date)
        # item_loader.add_value("parking", parking)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("energy_label", energy)

        # item_loader.add_value("latitude", lat)
        # item_loader.add_value("longitude", lng)

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        # # Monetary Status
        item_loader.add_value("rent", int(rent))
        item_loader.add_value("utilities", utils)
        item_loader.add_value("currency", "EUR")

        # # LandLord Details
        item_loader.add_value("landlord_name", 'IDEE & IMMOBILI')
        item_loader.add_value("landlord_phone", '055414890')
        # item_loader.add_value("landlord_email", landlord_email)

        yield item_loader.load_item()
